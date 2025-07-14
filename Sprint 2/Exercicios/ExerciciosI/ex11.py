# EX11 - Escreva uma função que recebe como parâmetro uma lista e retorna 3 listas:
# a lista recebida dividida em 3 partes iguais. Teste sua implementação com a lista abaixo
# lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

def dividir_funcao(list):
    tamanho = len(lista) // 3
    lista_um = lista[0:tamanho]
    lista_dois = lista[tamanho:2*tamanho]
    lista_tres = lista[2*tamanho:]
    return lista_um, lista_dois, lista_tres

lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
lista_um, lista_dois, lista_tres = dividir_funcao(lista)

print("Lista 1:", lista_um)
print("Lista 2:", lista_dois)
print("Lista 3:", lista_tres)
