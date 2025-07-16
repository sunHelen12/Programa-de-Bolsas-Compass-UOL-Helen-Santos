# EX05 - Leia o arquivo person.json, faça o parsing e imprima seu conteúdo.
# Dica: leia a documentação do pacote json

import json
import os

if __name__ == '__main__':
    diretorio_base = os.path.dirname(__file__)  
    caminho_arquivo = os.path.join(diretorio_base, "Arquivos", "person.json")
    
    with open(caminho_arquivo, "r", encoding="utf-8") as arquivo:
        dados = json.load(arquivo)
        print(dados)

