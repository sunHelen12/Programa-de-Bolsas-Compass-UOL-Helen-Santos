# Importando Bibliotecas Necessárias
import boto3
from datetime import datetime
import os 
from dotenv import load_dotenv 

# Carregando variáveis de ambiente
load_dotenv()

# Nomeando Bucket
NOME_BUCKET = 'data-lake-da-helen'
# Região usada
regiao = os.getenv('AWS_REGION')

# Diretório de arquivos CSV
DADOS_DIR = './arquivos'
# Arquivos para Upload
ARQUIVOS_UPLOAD = ['movies.csv', 'series.csv']

# Função para criar o bucket
def criar_bucket(nome_bucket):
    # Lança uma exceção se ocorrer um erro inesperado durante a criação    
    try:
        # Região padrão e não precisa da configuração de localização.        
        cliente_s3= boto3.client('s3', regiao)
        cliente_s3.create_bucket(Bucket=nome_bucket)
        print(f"Bucket '{nome_bucket}' criado com sucesso.")
       
    except boto3.client('s3').exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"Bucket '{nome_bucket}' já existe.")
        else:
            # Se for outro erro,  exibe-o e para a execução
            print(f"Erro inesperado ao criar o bucket: {e}")
            raise e

# Função para realizar upload de arquivos para a Amazon S3
def upload_s3(nome_bucket):
    print("Iniciando o processo de upload para o S3")
    cliente_s3 = boto3.client('s3', regiao)

    # Data para nomear estrutura de path
    hoje = datetime.now()
    ano = hoje.strftime("%Y")
    mes = hoje.strftime("%m")
    dia = hoje.strftime("%d")

    for nome_arquivo in ARQUIVOS_UPLOAD:
        # Caminho para o local dos arquivos
        caminho_arquivos = os.path.join(DADOS_DIR, nome_arquivo)
        # Etiquetando arquivos para seguirem os caminhos corretos
        if 'movies' in nome_arquivo.lower():
            spec = 'Movies'
        elif 'series' in nome_arquivo.lower():
            spec = 'Series'
        else:
            print(f"Arquivo '{nome_arquivo}' não reconhecido.")
            continue

        # Caminho para salvamento dos arquivos no S3 
        chave_s3 = f"Raw/Local/CSV/{spec}/{ano}/{mes}/{dia}/{nome_arquivo}"

        try:
            print(f"Enviando '{caminho_arquivos}' para o bucket '{NOME_BUCKET}'...")
            
            cliente_s3.upload_file(
                Filename=caminho_arquivos,
                Bucket=NOME_BUCKET,
                Key=chave_s3
            )

            print(f"Sucesso! Arquivo enviado para: s3://{NOME_BUCKET}/{chave_s3}\n")

        except Exception as e:
            print(f"Erro ao enviar o arquivo '{nome_arquivo}': {e}\n")

if __name__ == "__main__":
    # Chamando funções
    criar_bucket(NOME_BUCKET)
    upload_s3(NOME_BUCKET)
    print("Processo finalizado.")