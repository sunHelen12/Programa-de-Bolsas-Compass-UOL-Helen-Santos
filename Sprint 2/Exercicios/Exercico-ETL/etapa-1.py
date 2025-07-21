# Etapa 1 - Apresente o ator/atriz com maior número de filmes
# e a respectiva quantidade. A quantidade de filmes econtra-se 
# na coluna Number of Movies do arquivo.

import os

def etapa_1 (arquivo_csv, arquivo_txt):
    dados = []
    with open(arquivo_csv, "r") as arquivo:
        cabecalho = arquivo.readline().strip().split(',')
        for linha in arquivo:
            dados.append(linha.strip().split(','))

    
    indice_autor = cabecalho.index("Actor")
    indice_num_filmes = cabecalho.index("Number of Movies")
    
    max_filmes = -1
    atores_max = ""

    for registro in dados:
        try: 
            atores = registro[indice_autor]
            num_filmes = int(registro[indice_num_filmes])

            if num_filmes > max_filmes:
                max_filmes = num_filmes
                atores_max = atores
        except(ValueError, IndexError) as e:
            pass

    resultado = f"O ator/atriz que mais possui filmes no dataset é {atores_max} com {max_filmes} filmes."

    with open(arquivo_txt, "w") as etapa:
        etapa.write(resultado)

    print(f"Etapa 1 concluída. O resultado foi salvo em 'etapa-1.txt'!")
    print(resultado)

def main():
    diretorio_base = os.path.dirname(__file__)  
    arquivo_csv = os.path.join(diretorio_base, "actors.csv")
    arquivo_txt = os.path.join(diretorio_base, "etapa-1.txt")
    etapa_1(arquivo_csv, arquivo_txt)

if __name__ == "__main__":
   main()