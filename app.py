import io
import logging
import os

from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from pypdf import PdfWriter

load_dotenv()
logging.basicConfig(level=logging.INFO)
credentials = service_account.Credentials.from_service_account_file("credentials.json")

def list_pdf_files(bucket_name: str):
    """Lista e retorna todos os arquivos PDF no bucket, incluindo subpastas."""
    logging.info(f"Listando arquivos PDF no bucket '{bucket_name}'...")
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    pdf_files = [blob.name for blob in bucket.list_blobs() if blob.name.lower().endswith('.pdf')]
    logging.info(f"Encontrados {len(pdf_files)} arquivos PDF no bucket '{bucket_name}', incluindo subpastas.")
    return pdf_files


def download_pdf(bucket_name: str, file_name: str) -> io.BytesIO:
    """Faz o download de um PDF do bucket do Google Cloud e retorna como BytesIO."""
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    pdf_stream = io.BytesIO()
    blob.download_to_file(pdf_stream)
    pdf_stream.seek(0)
    logging.info(f"PDF '{file_name}' baixado do bucket '{bucket_name}'.")
    return pdf_stream


def compress_pdf(pdf_stream: io.BytesIO) -> io.BytesIO:
    """Comprime um PDF reduzindo a qualidade das imagens."""
    pdf_document = PdfWriter(clone_from=pdf_stream)

    for page in pdf_document.pages:
        page.compress_content_streams(level=9)


    logging.info("PDF comprimido com sucesso.")
    return pdf_document


def upload_pdf(bucket_name: str, file_name: str, pdf_stream: io.BytesIO):
    """Carrega o PDF de volta para o bucket, sobrescrevendo o arquivo antigo."""
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    pdf_stream.seek(0)
    blob.upload_from_file(pdf_stream, content_type="application/pdf")
    logging.info(f"PDF '{file_name}' enviado ao bucket '{bucket_name}' com sucesso.")


def process_all_pdfs(bucket_name: str):
    """Lista, baixa, comprime e envia novamente todos os PDFs no bucket, incluindo os que estão em subpastas."""
    pdf_files = list_pdf_files(bucket_name)
    for file_name in pdf_files:
        logging.info(f"Processando PDF '{file_name}'...")
        pdf_stream = download_pdf(bucket_name, file_name)
        compressed_pdf_stream = compress_pdf(pdf_stream)
        
        # upload_pdf(bucket_name, file_name, compressed_pdf_stream)
    logging.info("Processamento completo para todos os PDFs no bucket.")


if __name__ == "__main__":
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    if not BUCKET_NAME:
        raise ValueError("A variável de ambiente 'BUCKET_NAME' não foi definida.")
    try:
        process_all_pdfs(BUCKET_NAME)
    except KeyboardInterrupt:
        logging.info("Processamento interrompido pelo usuário.")
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")