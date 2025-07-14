# EX07 - Escreva um programa que lê o conteúdo do arquivo texto arquivo_texto.txt 
# e imprime o seu conteúdo.
# Dica: leia a documentação da função open(...)

import os

os.chdir(os.path.dirname(__file__))

with open("arquivo_texto.txt", "r", encoding="utf-8") as arquivo_texto:
    arquivo = arquivo_texto.readlines()
    for linha in arquivo:
        print(linha.strip())