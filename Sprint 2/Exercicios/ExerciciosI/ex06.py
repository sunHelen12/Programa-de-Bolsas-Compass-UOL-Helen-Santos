# EX06 - Implemente a função my_map(list, f) que recebe uma lista como primeiro argumento
# e uma função como segundo argumento. Esta função aplica a função recebida para cada 
# elemento da lista recebida e retorna o resultado em uma nova lista.
# Teste sua função com a lista de entrada [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] 
# e com uma função que potência de 2 para cada elemento.

def my_map(list, f):
    nova_lista = []
    for item in list:
        nova_lista.append(f(item))
    return nova_lista

def potencia_de_dois(x):
    return 2 ** x

if __name__ == '__main__':
    lista_de_entrada = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] 
    resultado = my_map(lista_de_entrada, potencia_de_dois)

    print(resultado)