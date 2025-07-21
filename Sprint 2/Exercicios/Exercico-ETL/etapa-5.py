import os

def etapa_5(arquivo_csv, arquivo_txt):
    atores_receita = []

    try:
        with open(arquivo_csv, "r", encoding="utf-8") as arquivo:
            cabecalho = arquivo.readline().strip().split(",")

            for linha in arquivo:
                if not linha.strip():
                    continue

                partes = linha.strip().split(",")

                while len(partes) > len(cabecalho):
                    partes[0] += "," + partes[1]
                    del partes[1]

                nome = partes[0].strip().replace('"', '')
                total_bruto = partes[1].strip().replace('"', '')

                bruto_limpo = float(total_bruto.replace("$", "").replace(",", ""))
                atores_receita.append((nome, bruto_limpo))

       
        atores_ordenados = sorted(atores_receita, key=lambda x: x[1], reverse=True)

        saidas = []

        with open(arquivo_txt, "w", encoding="utf-8") as etapa:
            for nome, receita in atores_ordenados:
                resultado = f"{nome} - ${receita:,.2f}\n"
                etapa.write(resultado)
                saidas.append(resultado)
        
        print(f"Etapa 5 concluída. O resultado foi salvo em '{arquivo_txt}'!")  

        for saida in saidas[:10]:  
            print(saida.strip())

    except Exception as e:
        print(f"Ocorreu um erro inesperado:{e}")

def main():
    diretorio_base = os.path.dirname(__file__)
    arquivo_csv = os.path.join(diretorio_base, "actors.csv")
    arquivo_txt = os.path.join(diretorio_base, "etapa-5.txt")
    etapa_5(arquivo_csv, arquivo_txt)

if __name__ == "__main__":
    main()
