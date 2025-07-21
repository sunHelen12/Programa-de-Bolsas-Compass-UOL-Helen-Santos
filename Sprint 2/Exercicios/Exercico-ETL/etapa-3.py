import os

def etapa_3(arquivo_csv, arquivo_txt):
    maior_media = 0.0
    ator_media = ""

    try: 
        with open(arquivo_csv, "r", encoding="utf-8") as arquivo:
            cabecalho = arquivo.readline().strip()
            colunas = cabecalho.split(",")

            indice_media = colunas.index("Average per Movie")
            
            for linha in arquivo:
                linha = linha.strip()
                if not linha:
                    continue

                partes = linha.split(",")

                while len(partes) > len(colunas):
                    partes[0] += "," + partes[1]
                    del partes[1]

                try:
                    media_atual = float(partes[indice_media])
                    if media_atual > maior_media:
                        maior_media = media_atual
                        ator_media = partes[0]
                except (ValueError, IndexError):
                    continue
            
        resultado = f"{ator_media} tem a maior média de bilheteria por filme: ${maior_media:.2f} milhões."

        with open(arquivo_txt, "w", encoding="utf-8") as etapa:
            etapa.write(resultado)  

        print(f"Etapa 3 concluída. O resultado foi salvo em 'etapa-3.txt'!")
        print(resultado)  

    except Exception as e:   
        print(f"Ocorreu um erro inesperado: {e}")


def main():
    diretorio_base = os.path.dirname(__file__)  
    arquivo_csv = os.path.join(diretorio_base, "actors.csv")
    arquivo_txt = os.path.join(diretorio_base, "etapa-3.txt")
    etapa_3(arquivo_csv, arquivo_txt)

if __name__ == "__main__":
    main()