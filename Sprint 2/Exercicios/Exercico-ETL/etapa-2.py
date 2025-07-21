import os

def etapa_2(arquivo_csv, arquivo_txt):
    total_receita = 0.0
    contador_filmes = 0

    try:
        with open(arquivo_csv, "r", encoding="utf-8") as arquivo:
            cabecalho = arquivo.readline().strip().split(",")
            coluna = cabecalho.index("Gross")                

            for linha in arquivo: 
               partes = linha.strip().split(",")

               while len(partes) > len(cabecalho):
                    partes[0] += "," + partes[1]
                    del partes[1]

               try:
                    valor_bruto = partes[coluna].strip().replace("$", "").replace(",", "")
                    if valor_bruto:
                        total_receita += float(valor_bruto)
                        contador_filmes += 1
               except ValueError:
                    continue
        
        media = total_receita / contador_filmes
        resultado = f"O número médio de filmes por ator é de {media:.2f}"

        with open(arquivo_txt, "w", encoding="utf-8") as etapa:
            etapa.write(resultado)

        print(f"Etapa 2 concluída. O resultado foi salvo em 'etapa-2.txt'!")
        print(resultado)

    except Exception as e:
        print(f"Ocorreu um erro inesperado:{e}")

def main():
    diretorio_base = os.path.dirname(__file__)  
    arquivo_csv = os.path.join(diretorio_base, "actors.csv")
    arquivo_txt = os.path.join(diretorio_base, "etapa-2.txt")
    etapa_2(arquivo_csv, arquivo_txt)

if __name__ == "__main__":
    main()