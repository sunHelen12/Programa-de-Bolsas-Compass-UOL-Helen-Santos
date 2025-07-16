# EX20 - Você está recebendo um arquivo contendo 10.000 números inteiros, um em cada linha. Utilizando 
# lambdas e high order functions, apresente os 5 maiores valores pares e a soma destes.
# Você deverá aplicar as seguintes funções no exercício:
# map
# filter
# sorted
# sum
# Seu código deverá exibir na saída (simplesmente utilizando 2 comandos `print()`):
# a lista dos 5 maiores números pares em ordem decrescente;
# a soma destes valores.

import os

if __name__ == "__main__":    
    diretorio_base = os.path.dirname(__file__)  
    caminho_arquivo = os.path.join(diretorio_base, "Arquivos", "number.txt")

    with open(caminho_arquivo, "r") as arquivo:    
        numeros_str = arquivo.readlines()

    numeros_int = map(int, numeros_str)
    numeros_pares = filter(lambda x: x % 2 == 0, numeros_int)
    numeros_pares_ordenados = sorted(numeros_pares, reverse=True)
    maiores_pares = numeros_pares_ordenados[:5]
    soma_maiores_pares = sum(maiores_pares)

    print(maiores_pares)
    print(soma_maiores_pares)
