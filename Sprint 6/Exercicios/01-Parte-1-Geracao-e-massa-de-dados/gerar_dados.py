# Importando Bibliotecas Necessárias
import random
import time
import os 
import pathlib
import names

# Etapa 1 - Geração de números aleatórios

# Lista que gera 250 números aleatórios
numeros_aleatorios = [random.randint(1, 1000) for _ in range(250)]
# Imprime os resultados 
print(100*'=')
print(f"""
                                 Lista Original
      """
    )
print(100*'=')
print(numeros_aleatorios)

# Método Reverso sobre a lista
numeros_aleatorios.reverse()
# Imprime os resulltados
print(100*'=')
print(f"""                            
                                    Lista Reversa
      """
      )
print(100*'=')
print(numeros_aleatorios)

# Quebra de linha
print()

# Etapa 2 - Listagem de Animais

# Lista com nome de 20 animais
animais = [
    "Gato", "Coelho", "Cachorro", "Raposa", "Tamanduá", "Tigre", "Cavalo", "Lhama", "Coruja","Urso",
    "Chinchilla", "Furão", "Zebra", "Girafa", "Leão", "Coala", "Arara", "Beija-Flor", "Abelha", "Esquilo"
]

# Ordenação da lista em ordem alfabética
animais.sort()

# Iteração dos nomes e impressão deles
print(100*'=')
print(f"""
                                  Lista Animais em Ordem Alfabética
      """
    )
print(100*'=')
[print(animal) for animal in animais]

# Pasta para receber arquivo de texto
pasta_respostas = pathlib.Path('respostas')
# Evita erros se a pasta já existir
pasta_respostas.mkdir(exist_ok=True)

# Armazenamento de de conteúdo em arquivo de texto
arquivo_animais = pasta_respostas / "lista_animais.txt"
with open(arquivo_animais, 'w', encoding='utf-8') as arquivo:
    arquivo.write("============Lista de Animais============\n")
    for animal in animais:
        arquivo.write(f"{animal}\n")

print(f"\nLista de animais salva no arquivo '{arquivo_animais}'.")

# Etapa 3 - Geração de Nomes Aleatórios

# Configurando Aleatoriedade
random.seed(40)
# Quantidade de nomes únicos
qtd_nomes_unicos= 39080
# Quantidade de nomes aleatórios
qtd_nomes_aleatorios = 10000000

# Gera lista de nomes únicos
print(100*'=')
print(f"""
                    Nomes Únicos
      """)
print(100*'=')
print(qtd_nomes_unicos)

aux = []
for i in range(qtd_nomes_unicos):
    aux.append(names.get_full_name())

print("Base de nomes únicos criada.")

# Gera a lsita final
print(f"Gerando {qtd_nomes_aleatorios} nomes aleatórios...")
dados = []
for i in range(qtd_nomes_aleatorios):
  dados.append(random.choice(aux))

# Gera arquivo de texto com os nomes
arquivo_nomes = pasta_respostas / "nomes_aleatorios.txt"
print(f"Escrevendo nomes no arquivo '{arquivo_nomes}'...")
tempo_inicio = time.time()

with open(arquivo_nomes, 'w', encoding='utf-8') as arquivo:
    arquivo.write("==========Lista Nomes==========\n")
    for nome in dados:
        arquivo.write(f"{nome}\n")

tempo_fim = time.time()
tempo_total = tempo_fim - tempo_inicio

print(f"Arquivo '{arquivo_nomes}' gerado com sucesso!")
print(f"Tempo de escrita no arquivo: {tempo_total:.2f} segundos.")