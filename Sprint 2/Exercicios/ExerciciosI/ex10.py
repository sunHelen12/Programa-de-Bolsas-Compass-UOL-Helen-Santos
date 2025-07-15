# EX10 - Escreva uma função que recebe uma string de números separados por vírgula e
# retorne a soma de todos eles. Depois imprima a soma dos valores.
# A string deve ter valor  "1,3,4,6,10,76"
# Importante: Esperamos que você utilize split() em seu código.

valor = "1,3,4,6,10,76"

def soma_numeros(string):
    numeros = string.split(',')
    soma = 0
    for num in numeros:
        soma += int(num)
    return soma

if __name__ == '__main__':
    resultado = soma_numeros(valor)
    print("Soma:", resultado)