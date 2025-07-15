# EX12 - Dado o dicionário a seguir:
# speed = {'jan':47, 'feb':52, 'march':47, 'April':44, 'May':52, 'June':53, 'july':54, 'Aug':44, 'Sept':54}
# Crie uma lista com todos os valores (não as chaves!) e coloque numa lista de forma que não haja valores duplicados.

if __name__ == '__main__':
    speed = {'jan':47, 'feb':52, 'march':47, 'April':44, 'May':52, 'June':53, 'july':54, 'Aug':44, 'Sept':54}

    valores = speed.values()
    valores_lista = list(speed.values())
    valores_unicos = list(set(valores_lista))

    print("Valores únicos:", valores_unicos)