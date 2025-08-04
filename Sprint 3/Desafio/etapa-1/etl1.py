# Importando Bibliotecas
import pandas as pd
from pathlib import Path

# Rodando localmente ou Docker
if Path('/app/dados').exists():
    # Ambiente Docker
    pasta_dados = Path('/app/dados')
else:
    # Ambiente Local
    pasta_dados = Path(__file__).resolve().parent.parent / "volume"

# Caminhos de entrada e saída
caminho_entrada = pasta_dados / "concert_tours_by_women.csv"
caminho_saida = pasta_dados / "csv_limpo.csv"

# Garante que a pasta de saída existe
pasta_dados.mkdir(parents=True, exist_ok=True)

# Fazendo a leitura do arquivo e carregando DataFrame 
df = pd.read_csv(caminho_entrada)

# Imprimindo dados originais
titulo = " Dados Originais " 
print(titulo.center(150, '='))
print(df.to_string(index=False))
print()

# PROCESSO DE LIMPEZA

# Deixando as colunas que queremos manter
colunas_mantidas=[
    'Rank',
    'Actual gross',
    'Adjustedgross (in 2022 dollars)',
    'Artist',
    'Tour title',
    'Shows',
    'Average gross',
    'Year(s)',
]
# Criando uma cópia com as colunas e os dados que serão utilizados
df_modificado = df[colunas_mantidas].copy()
print()

# Visualizando modificação
titulo = " Colunas " 
print(titulo.center(150, '='))
print(df_modificado.to_string(index=False))
print()

# Limpando a coluna 'Tour tiltle' - Removendo caracteres 
df_modificado['Tour title'] = df_modificado['Tour title'].str.replace(r'†|‡|\[.*?\]|\*', '', regex=True).str.strip()

# Limpando as colunas 'Actual gross', 'Adjusted_gross_2022' e 'Average_gross'
colunas_valores = [
    'Actual gross',
    'Adjustedgross (in 2022 dollars)',
    'Average gross'
]

for coluna in colunas_valores:
    # Valores em String
    df_modificado[coluna] = df_modificado[coluna].astype(str)

    # Removendo $, (,), [] e letras
    df_modificado[coluna] = df_modificado[coluna].str.replace(r'[$,\[\]a-zA-Z]', '', regex=True).str.strip()

    # Convertendo coluna para valores do tipo (float)
    df_modificado[coluna] = pd.to_numeric(df_modificado[coluna], errors='coerce')  

    # Formata para mostrar uma casa decimal
    df_modificado[coluna] = df_modificado[coluna].map(lambda x: f"{x:.1f}" if pd.notnull(x) else "")

# Limpando coluna 'Shows' - Conversão para valor númerico
df_modificado['Shows'] = pd.to_numeric(df_modificado['Shows'], errors='coerce')

# Convertendo valores da coluna 'Year(s)' para String
df_modificado['Year(s)'] = df_modificado['Year(s)'].astype(str)

# Dividindo  a coluna Year(s) nas colunas 'Start year' e 'End year'
df_modificado[['Start year', 'End year']] = df_modificado['Year(s)'].str.split('-', n=1, expand=True)

# Preenchendo as linhas vazias de 'End year' com o valor da 'Start year'
df_modificado['End year'] = df_modificado['End year'].fillna(df_modificado['Start year'])

# Removendo coluna 'Year(s) 
df_modificado.drop(columns=['Year(s)'], inplace=True)

# Removendo possíveis espaços em branco que possam ter sobrado no início ou fim dos anos
df_modificado['Start year'] = df_modificado['Start year'].str.strip()
df_modificado['End year'] = df_modificado['End year'].str.strip()

# Título no print do DataFrame
titulo = " Colunas e Valores Formatados " 
print(titulo.center(150, '='))
print(df_modificado.to_string(index=False))
print()

# Salvando DataFrame  limpo em um novo arquivo
df_modificado.to_csv(caminho_saida, index=False)