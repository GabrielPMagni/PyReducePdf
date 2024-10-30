# PyReducePDF

PyReducePDF é uma ferramenta para listar, baixar, comprimir e enviar PDFs de um bucket do Google Cloud. Este projeto foi desenvolvido para facilitar a manipulação de arquivos PDF armazenados na nuvem, reduzindo seu tamanho e mantendo a qualidade.

## Funcionalidades

- Listar todos os arquivos PDF em um bucket, incluindo subpastas.
- Baixar arquivos PDF do bucket.
- Comprimir PDFs reduzindo a qualidade das imagens.
- Enviar PDFs comprimidos de volta para o bucket, sobrescrevendo os arquivos antigos.
- Salvar PDFs comprimidos localmente, se desejado.

## Requisitos

- Python 3.6 ou superior
- Google Cloud SDK
- Bibliotecas Python:
    - `google-cloud-storage`
    - `pypdf`
    - `python-dotenv`

## Instalação

1. Clone o repositório:
        `git clone https://github.com/GabrielPMagni/PyReducePDF.git`
        `cd PyReducePDF`

2. Crie um ambiente virtual e ative-o:
        `python3 -m venv venv`
        `source venv/bin/activate`

3. Instale as dependências:
        `pip install -r requirements.txt`

4. Configure suas credenciais do Google Cloud:
        - Coloque o arquivo `credentials.json` na raiz do projeto.

## Uso

Execute o script principal com o nome do bucket e as opções desejadas:
```sh
python3 app.py [bucket_name] [options]
```

### Opções

- `-s`, `--store`: Salva os PDFs localmente em vez de enviá-los de volta para o bucket.
- `-v`, `--verbose`: Exibe mensagens de debug.

### Exemplo

Para processar todos os PDFs no bucket `meu-bucket` e salvá-los localmente:
```sh
python3 app.py meu-bucket --store --verbose
```

## Estrutura do Projeto

- `pdfs/`: Diretório contendo os PDFs baixados do bucket.
- `app.py`: Script principal contendo a lógica de processamento dos PDFs.
- `credentials.json`: Arquivo de credenciais do Google Cloud.
- `execution.log`: Arquivo de log gerado durante a execução.

## Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues e pull requests.

## Licença

Este projeto está licenciado sob a [MIT License](LICENSE).

## Autor

Desenvolvido por: Gabriel P. Magni
