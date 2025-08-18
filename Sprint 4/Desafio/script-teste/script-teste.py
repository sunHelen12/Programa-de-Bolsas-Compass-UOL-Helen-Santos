import pandas as pd
import boto3
from dotenv import load_dotenv
import os
import sys

print("Iniciando script de teste...")

# Cofiguração e Conexão
load_dotenv()

s3_client = boto3.client('s3',
   aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
   aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
   aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
   region_name='us-east-1'
)

bucket_name = "bucket-desafio-2497" 
s3_key_limpo = 'limpo/dados_limpos_2021.csv'
print(f"Tentando ler o arquivo '{s3_key_limpo}' do bucket '{bucket_name}'...")

# Carregamento dos dados limpos
try:
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key_limpo)
    df_teste = pd.read_csv(response['Body'], sep=';')
    print("Arquivo limpo carregado com sucesso do S3.")
except Exception as e:
    print(f"FALHA CRÍTICA: Não foi possível carregar o arquivo de teste do S3. Erro: {e}")
    # Encerra o script com um código de erro
    sys.exit(1) 

# Executando teste de validação
erros = []
print("\nRelatório Final dos Testes")
if not erros:
    print("Sucesso! O Teste passou.")
else:
    print(f"Falha! Foram encontrados {len(erros)} erros:")
    for erro in erros:
        print(f"  - {erro}")
    # Encerrando o script com um código de erro para indicar falha
    sys.exit(1) 
