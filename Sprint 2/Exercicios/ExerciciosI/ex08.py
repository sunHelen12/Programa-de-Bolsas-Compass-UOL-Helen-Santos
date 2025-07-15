# EX08 - Escreva uma função que recebe um número variável de parâmetros não 
# nomeados e um número variado de parâmetros nomeados e imprime o valor de cada 
# parâmetro recebido.
# Teste sua função com os seguintes parâmetros:
# (1, 3, 4, 'hello', parametro_nomeado='alguma coisa', x=20)

def mostrar_parametros(*posicional, **nomeado):
    print("Parâmetros posicionais:")
    for i, arg in enumerate(posicional):
        print(f"  {i} → {arg}")
    
    print("\nParâmetros nomeados:")
    for chave, valor in nomeado.items():
        print(f"  {chave} → {valor}")

if __name__ == '__main__':
    mostrar_parametros(1, 3, 4, 'hello', parametro_nomeado='alguma coisa', x=20)
