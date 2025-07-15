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
    os.chdir(os.path.dirname(__file__))

    with open("numeros.txt", "r", encoding="utf-8") as arquivo:
        linhas = arquivo.readlines()
    
    numeros = list(map(int, linhas))
    pares = list(filter(lambda x: x % 2 == 0, numeros))
    maiores_pares = sorted(pares, reverse=True)[:5]
    soma = sum(maiores_pares)

    print(maiores_pares)
    print(soma)
