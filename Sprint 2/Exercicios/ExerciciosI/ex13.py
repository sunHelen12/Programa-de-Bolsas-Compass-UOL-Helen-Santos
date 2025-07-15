# EX13 - Calcule o valor mínimo, valor máximo, valor médio e a mediana da lista gerada na célula abaixo:
# Obs.: Lembrem-se, para calcular a mediana a lista deve estar ordenada!
# import random
# amostra aleatoriamente 50 números do intervalo 0...500
# random_list = random.sample(range(500),50)
# Use as variáveis abaixo para representar cada operação matemática:
# mediana
# media
# valor_minimo
# valor_maximo
# - Importante: Esperamos que você utilize as funções abaixo em seu código:
# - random
# - max
# - min
# - sum

import random

if __name__ == '__main__':
    random_list = random.sample(range(500), 50)
    valor_minimo = min(random_list)
    valor_maximo = max(random_list)

    media = sum(random_list) / len(random_list)
    lista_ordenada = sorted(random_list)
    meio = len(lista_ordenada) // 2
    mediana = (lista_ordenada[meio - 1] + lista_ordenada[meio]) / 2

    print("Mediana:", mediana)
    print("Média:", media)
    print("Valor Mínimo:", valor_minimo)
    print("Valor Máximo:", valor_maximo)



