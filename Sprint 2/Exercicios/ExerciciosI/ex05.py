# EX05 - Leia o arquivo person.json, faça o parsing e imprima seu conteúdo.
# Dica: leia a documentação do pacote json

import json
import os

if __name__ == '__main__':
    os.chdir(os.path.dirname(__file__))
    with open("person.json", "r", encoding="utf-8") as arquivo:
        dados = json.load(arquivo)
        print(dados)

