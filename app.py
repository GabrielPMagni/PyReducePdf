import argparse
from io import BytesIO
import logging
import os

from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from pypdf import PdfWriter, PdfReader

load_dotenv()

logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG) 

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG) 

console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

file_handler = logging.FileHandler("execution.log")
file_handler.setLevel(logging.WARNING)

file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

credentials = service_account.Credentials.from_service_account_file("credentials.json")

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

def list_pdf_files(bucket_name: str):
    """Lista e retorna todos os arquivos PDF no bucket, incluindo subpastas."""
    logger.info(f"Listando arquivos PDF no bucket '{bucket_name}'...") if args.verbose else None
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    pdf_files = [blob.name for blob in bucket.list_blobs() if blob.name.lower().endswith('.pdf')]
    logger.warning(f"Encontrados {len(pdf_files)} arquivos PDF no bucket '{bucket_name}', incluindo subpastas.")
    return pdf_files


def download_pdf(bucket_name: str, file_name: str) -> BytesIO:
    """Faz o download de um PDF do bucket do Google Cloud e retorna como BytesIO."""
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    pdf_stream = BytesIO()
    blob.download_to_file(pdf_stream)
    pdf_stream.seek(0)
    logger.info(f"PDF '{file_name}' baixado do bucket '{bucket_name}'.") if args.verbose else None
    return pdf_stream


def compress_pdf(pdf_stream: BytesIO) -> PdfReader:
    """Comprime um PDF reduzindo a qualidade das imagens."""
    pdf_document = PdfWriter(clone_from=pdf_stream)

    for page in pdf_document.pages:
        page.compress_content_streams(level=9)

    reader = PdfReader(pdf_stream)
    logger.info("PDF comprimido com sucesso.") if args.verbose else None
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
        logger.info(f"PDF '{file_name}' enviado ao bucket '{bucket_name}' com sucesso.") if args.verbose else None
    except Exception as e:
        logger.error(f"Erro ao enviar PDF '{file_name}' ao bucket '{bucket_name}': {e}")


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
            logger.info(f"PDF '{file_name}' salvo localmente.") if args.verbose else None
        except Exception as e:
            logger.error(f"Erro ao salvar PDF '{file_name}' localmente: {e}")


def process_all_pdfs(bucket_name: str):
    """Lista, baixa, comprime e envia novamente todos os PDFs no bucket, incluindo os que estão em subpastas."""
    pdf_files = list_pdf_files(bucket_name)
    counter = 0
    for file_name in pdf_files:
        counter += 1
        if counter > 2:
            break
        logger.info(f"Processando PDF '{file_name}'...") if args.verbose else None
        pdf_stream = download_pdf(bucket_name, file_name)
        compressed_pdf_stream = compress_pdf(pdf_stream)
        if args.store:
            store_file_locally(compressed_pdf_stream, file_name)
        else:
            upload_pdf(bucket_name, file_name, compressed_pdf_stream)
    logger.info("Processamento completo para todos os PDFs no bucket.") if args.verbose else None

if __name__ == "__main__":
    BUCKET_NAME = args.bucket_name
    if not BUCKET_NAME:
        raise ValueError("'BUCKET_NAME' não foi definida.")
    try:
        process_all_pdfs(BUCKET_NAME)
    except KeyboardInterrupt:
        logger.info("Processamento interrompido pelo usuário.")
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")