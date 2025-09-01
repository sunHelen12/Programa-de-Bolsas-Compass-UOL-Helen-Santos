# Bibliotecas necessárias
import requests
import pandas as pd
from IPython.display import display
import os  
from dotenv import load_dotenv
import json
import pathlib

# Carregando a variável definida no arquivo .env
load_dotenv()

# Lendo o valor da variável de ambiente "api_key"
api_key = os.getenv("api_key")
# Define o endpoint específico da API - Filmes mais bem avaliados
endpoint = "movie/top_rated"
# Definindo parâmetros adicionais para a requisição - Idioma da resposta
parametros_opcionais = "language=pt-BR"

# URl da requisição
url = f"https://api.themoviedb.org/3/{endpoint}?api_key={api_key}&{parametros_opcionais}"

# Pasta que receberá os dados
pasta_resposta = pathlib.Path('resposta')
# Evitando erros se a pasta já existir
pasta_resposta.mkdir(exist_ok=True)

# Nomeando e definindo caminho para arquivo
nome_arquivo = pasta_resposta / 'filmes_mais_avaliados.json'

# Envia requisição
response = requests.get(url)
# Convertendo respsota no formato JSON
data = response.json()
# Imprime no console identada e formatada
print(json.dumps(data, indent=4, ensure_ascii=False))

# Salvando dados no arquivo  
with open(nome_arquivo, 'w', encoding='utf-8') as arquivo:
        json.dump(data, arquivo, ensure_ascii=False, indent=4)
        
print(f"Dados salvos com sucesso!")
