import os

def etapa_4(arquivo_csv, arquivo_txt):
    contagem_filme = {}    

    try:
        with open(arquivo_csv, "r", encoding="utf-8") as arquivo:
            cabecalho = arquivo.readline().strip()
            colunas = cabecalho.split(",")

            indice_filme = colunas.index("#1 Movie")

            for linha in arquivo:
                linha = linha.strip()
                if not linha:
                    continue
                
                partes = linha.split(",")

                while len(partes) > len(colunas):
                    partes[0] += "," + partes[1]
                    del partes[1]

                try:
                    filme = partes[indice_filme].strip()
                    if filme:
                        contagem_filme[filme] = contagem_filme.get(filme, 0) + 1
                except IndexError:
                    continue

            filmes_ordenados = sorted(contagem_filme.items(), key=lambda x: (-x[1], x[0]))

        saidas = []

        with open(arquivo_txt, "w", encoding="utf-8") as etapa:
            for filme, contagem in filmes_ordenados:
                resultado = f"O filme {filme} aparece {contagem} vez(es) no dataset.\n"
                etapa.write(resultado)
                saidas.append(resultado)

        print(f"Etapa 4 concluída. O resultado foi salvo em 'etapa-4.txt'!")   
       
        for saida in saidas:
            print(saida.strip())

    except Exception as e:
        print(f"Ocorreu um erro inesperado:{e}")

def main():
    diretorio_base = os.path.dirname(__file__)  
    arquivo_csv = os.path.join(diretorio_base, "actors.csv")
    arquivo_txt = os.path.join(diretorio_base, "etapa-4.txt")
    etapa_4(arquivo_csv, arquivo_txt)

if __name__ == "__main__":
    main()