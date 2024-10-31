import argparse
from io import BytesIO
import logging
import os
import json

from google.cloud import storage
from google.oauth2 import service_account
from pypdf import PdfWriter, PdfReader

def load_credentials():
    """Carrega as credenciais do arquivo JSON."""
    return service_account.Credentials.from_service_account_file("credentials.json")

parser = argparse.ArgumentParser(
                    prog='PyReducePDF',
                    description='Obtem, comprime e envia PDFs de um bucket do Google Cloud.',
                    epilog='Desenvolvido por: Gabriel P. Magni',
                    usage='python3 app.py [bucket_name] [options]'
                    )

parser.add_argument('bucket_name', type=str, help='Nome do bucket no Google Cloud.')
parser.add_argument('-s', '--store', action='store_true', help='Salva os PDFs localmente.')
parser.add_argument('-v', '--verbose', action='store_true', help='Exibe mensagens de debug.')

args = parser.parse_args()


def log(message, level: int = logging.INFO):
    """Loga uma mensagem."""
    if level <= logging.INFO and args.verbose:
        logger.info(message)
    else:
        logger.log(level, message)


def list_pdf_files(bucket_name: str):
    """Lista e retorna todos os arquivos PDF no bucket, incluindo subpastas."""
    log(f"Listando arquivos PDF no bucket '{bucket_name}'...")
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    pdf_files = [blob.name for blob in bucket.list_blobs() if blob.name.lower().endswith('.pdf')]
    log(f"Encontrados {len(pdf_files)} arquivos PDF no bucket '{bucket_name}', incluindo subpastas.", logging.WARNING)
    log('Arquivos encontrados:', logging.WARNING)
    log('---------------------', logging.WARNING)
    log(pdf_files, logging.WARNING)
    log('---------------------', logging.WARNING)
    return pdf_files


def download_pdf(bucket_name: str, file_name: str) -> BytesIO:
    """Faz o download de um PDF do bucket do Google Cloud e retorna como BytesIO."""
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    pdf_stream = BytesIO()
    blob.download_to_file(pdf_stream)
    pdf_stream.seek(0)
    log(f"PDF '{file_name}' baixado do bucket '{bucket_name}'.")
    return pdf_stream


def compress_pdf(pdf_stream: BytesIO) -> PdfReader:
    """Comprime um PDF reduzindo a qualidade das imagens."""
    pdf_document = PdfWriter(pdf_stream)

    for page in pdf_document.pages:
        page.compress_content_streams(level=9)

    pdf_document.compress_identical_objects(remove_identicals=True, remove_orphans=True)
    pdf_document.metadata = None
    
    tmp_stream = BytesIO()
    pdf_document.write(tmp_stream)
    reader = PdfReader(tmp_stream)
    
    pdf_document.close()
    
    log("PDF comprimido com sucesso.")
    return reader


def upload_pdf(bucket_name: str, file_name: str, pdf_stream: PdfReader):
    """Carrega o PDF de volta para o bucket, sobrescrevendo o arquivo antigo."""
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    try:
        stream = pdf_stream.stream
        stream.seek(0)
        blob.upload_from_file(stream, content_type="application/pdf")
        log(f"PDF '{file_name}' enviado ao bucket '{bucket_name}' com sucesso.")
    except Exception as e:
        log(f"Erro ao enviar PDF '{file_name}' ao bucket '{bucket_name}': {e}", logging.ERROR)


def store_file_locally(pdf: PdfReader, file_name: str):
    """Salva um arquivo localmente."""
    default_folder = "pdfs"
    if not os.path.exists(default_folder):
        os.makedirs(default_folder)
    formatted_file_name = file_name.replace("/", "_")
    writer = PdfWriter(pdf)
    with open(os.path.join(default_folder, formatted_file_name), "wb") as file:
        try:
            writer.write(file)
            log(f"PDF '{file_name}' salvo localmente.")
        except Exception as e:
            log(f"Erro ao salvar PDF '{file_name}' localmente: {e}", logging.ERROR)


def process_pdf(bucket_name: str, file_name: str):
    """Baixa, comprime e envia um PDF do bucket."""
    pdf_stream = download_pdf(bucket_name, file_name)
    compressed_pdf_stream = compress_pdf(pdf_stream)
    if args.store:
        store_file_locally(compressed_pdf_stream, file_name)
    else:
        upload_pdf(bucket_name, file_name, compressed_pdf_stream)


def process_all_pdfs(bucket_name: str):
    """Lista, baixa, comprime e envia novamente todos os PDFs no bucket, incluindo os que estão em subpastas."""
    pdf_files = list_pdf_files(bucket_name)
    save_jobs(pdf_files)
    for file_name in pdf_files:
        log(f"Processando PDF '{file_name}'...")
        try:
            process_pdf(bucket_name, file_name)
            save_successful_job(file_name)
        except Exception as e:
            log(f"Erro ao processar PDF '{file_name}': {e}", logging.ERROR)
            save_failed_job(file_name, str(e))
    log("Processamento completo para todos os PDFs no bucket.")


def save_jobs(file_names: list[str]):
    """Salva os nomes dos arquivos em um arquivo JSON."""
    with open("jobs.json", "w", encoding='utf-8') as file:
        json.dump(file_names, file, indent=4, ensure_ascii=False)


def save_successful_job(file_name: str):
    """Salva o nome de um arquivo que foi processado com sucesso."""
    with open(successful_jobs_file, "a", encoding='utf-8') as file:
        file.write(file_name + "\n")


def save_failed_job(file_name: str, error: str):
    """Salva o nome de um arquivo que falhou no processamento."""
    if os.path.getsize(failed_jobs_file) == 0:
        failed_jobs = []
    else:
        with open(failed_jobs_file, "r", encoding='utf-8') as file:
            failed_jobs = json.load(file)
    payload = { "file_name": file_name, "error": error }
    failed_jobs.append(payload)
    with open(failed_jobs_file, "w", encoding='utf-8') as file:
        json.dump(failed_jobs, file, indent=4, ensure_ascii=False)


def handle_failed_jobs():
    """Lida com os arquivos que falharam no processamento."""
    with open("failed_jobs.json", "r", encoding='utf-8') as file:
        try:
            failed_jobs = json.load(file)
        except json.JSONDecodeError:
            failed_jobs = []
        for job in failed_jobs:
            log(f"Reprocessando arquivo '{job['file_name']}'...", logging.WARNING)
            try:
                process_pdf(args.bucket_name, job['file_name'])
                save_successful_job(job)
            except Exception as e:
                log(f"Erro ao reprocessar arquivo '{job['file_name']}': {e}", logging.ERROR)
            log("Reprocessamento completo para todos os arquivos que falharam.")


def clear_logs():
    """Limpa os arquivos de log."""
    with open(execution_log_file, "w") as file:
        file.write("")
        file.close()

    with open(failed_jobs_file, "w") as file:
        file.write("")
        file.close()

    with open(successful_jobs_file, "w") as file:
        file.write("")
        file.close()


def setup_logger():
    """Configura o logger."""
    logger = logging.getLogger("logger")
    logger.setLevel(logging.DEBUG) 

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG) 

    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    file_handler = logging.FileHandler(execution_log_file)
    file_handler.setLevel(logging.WARNING)

    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


credentials = load_credentials()
failed_jobs_file = 'failed_jobs.json'
execution_log_file = 'execution.log'
successful_jobs_file = 'successful_jobs.log'
logger = setup_logger()


if __name__ == "__main__":
    try:
        clear_logs()
        process_all_pdfs(args.bucket_name)
        # handle_failed_jobs()
    except KeyboardInterrupt:
        log("Processamento interrompido pelo usuário.")
    except Exception as e:
        log(f"Erro inesperado: {e}", logging.ERROR)