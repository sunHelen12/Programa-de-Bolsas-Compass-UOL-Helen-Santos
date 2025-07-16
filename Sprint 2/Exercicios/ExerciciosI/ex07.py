# EX07 - Escreva um programa que lê o conteúdo do arquivo texto arquivo_texto.txt 
# e imprime o seu conteúdo.
# Dica: leia a documentação da função open(...)

import os

if __name__ == '__main__':
    diretorio_base = os.path.dirname(__file__)  
    caminho_arquivo = os.path.join(diretorio_base, "Arquivos", "arquivo_texto.txt")

    with open(caminho_arquivo, "r", encoding="utf-8") as arquivo_texto:
        arquivo = arquivo_texto.readlines()
        for linha in arquivo:
            print(linha.strip())