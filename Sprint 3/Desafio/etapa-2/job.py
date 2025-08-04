# Importando Biblioteca necessárias
import pandas as pd
# Importação da biblioteca Matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib
# Importando a classe Path
from pathlib import Path 

# Usando backend não interativo
matplotlib.use('Agg')

# Detecta ambiente
if Path('/app/dados').exists():
    # Ambiente Docker     
    pasta_volume = Path('/app/dados')
else:
    # Ambiente Local   
    pasta_volume = Path(__file__).resolve().parent.parent / "volume"


# Cria a pasta de saída se necessário
pasta_volume.mkdir(parents=True, exist_ok=True)

# Caminhos de entrada e saída
caminho_entrada = pasta_volume / "csv_limpo.csv"

# Lendo o arquivo
df_limpo = pd.read_csv(caminho_entrada)

# Q1. Procurando artista mais frequente e a maior média de seu faturamento bruto

# Contando a aparição dos artistas
contar_artistas = df_limpo['Artist'].value_counts()
# Encontrando o número máximo de aparições
maximo_aparicoes = contar_artistas.max()
# Obtendo todos os artistas com número máximo de aparições
artista_frequentes = contar_artistas[contar_artistas == maximo_aparicoes].index.tolist()

# Inicializando as variáveis 
top_artista = None
maximo_faturamento = 0

for artista in artista_frequentes:
    # Filtrando as turnês do artista
    df_artista = df_limpo[df_limpo['Artist'] == artista]
    # Calculando a média do faturamento bruto
    media_faturamento = df_artista['Actual gross'].mean()

    # Verificando se artista tem uma média de faturamento maior que a máxima atual
    if media_faturamento > maximo_faturamento:
        maximo_faturamento = media_faturamento
        top_artista = artista

resposta_q1 = (
    f"A artista que mais aparece na lista é '{top_artista}. A média de seu faturamento bruto (Actual Gross) é de ${maximo_faturamento:.2f}."
)

print(resposta_q1)

# Q2. Turnê que aconteceu dentro de um ano com a maior média de faturamento bruto (Average gross)

# Convertando valores para String
df_limpo['Start year'] = df_limpo['Start year'].astype(str)
df_limpo['End year'] = df_limpo['End year'].astype(str)
# Filtrando o DataFrame para pegar apenas as linhas onde o ano de início é igual ao ano de fim
df_um_ano = df_limpo[df_limpo['Start year'] == df_limpo['End year']].copy()
# Encontra o índice da linha que contém o maior valor na coluna 'Average gross'
turne_maior_media = df_um_ano.loc[df_um_ano['Average gross'].idxmax()]
# Imprimindo a resposta
resposta_q2 = f"A turnê em um único ano com maior média de faturamento bruto é '{turne_maior_media['Tour title']}' da artista '{turne_maior_media['Artist']}'."
print(resposta_q2)

# Q3. As 3 turnês que possuem o show mais lucrativo

# Criando coluna para armazenar o resultado da divisão do faturamento ajustado pela quantidade de shows
df_limpo['Lucro por show'] = df_limpo['Adjustedgross (in 2022 dollars)'] / df_limpo['Shows']
# Ordenando e buscando as três primeiras linhas com as 3 turnês mais lucrativas
top_3 = df_limpo.sort_values(by='Lucro por show', ascending=False).head(3)

# Lista vazia para guardar as respostas formatadas
resposta_q3_lista = []

# Loop que passa por cada uma das 3 linhas do DataFrame 'top_3'
for index, row in top_3.iterrows():
    # Formatando valor do lucro por show 
    lucro_formatado = f"${row['Lucro por show']:.2f}"
    
    # Criando texto para cada turnê e a adiciona na lista
    resposta_q3_lista.append(
       f"- Artista: {row['Artist']}, Turnê: {row['Tour title']}, Valor por Show: {lucro_formatado}"
    )

# Juntando as 3 linhas de texto da lista, separadas por uma quebra de linha
resposta_q3 = "\n".join(resposta_q3_lista)
print(resposta_q3)

# Gerando Arquivo com Respostas
respostas = f"""
Q1:
---{resposta_q1}

Q2:
---{resposta_q2}

Q3:
---
{resposta_q3}
"""

# Salvando o arquivo na pasta de saída correta
caminho_saida = pasta_volume / "respostas.txt"
with open(caminho_saida, 'w', encoding='utf-8') as arquivo:
    arquivo.write(respostas.strip())

# Q4. Gráfico de linhas para a artista que mais aparece

# Filtrando o DataFrame para a artista mais frequente
df_artista_top = df_limpo[df_limpo['Artist'] == top_artista].copy()
# Garantindo que 'Start year' seja numérico
df_artista_top['Start year'] = pd.to_numeric(df_artista_top['Start year'], errors='coerce')
# Agrupando as turnês por ano e somando o faturamento de cada ano
faturamento_ano = df_artista_top.groupby('Start year')['Actual gross'].sum()

# Criando a figura e os eixos para o gráfico
plt.figure(figsize=(12, 9))
# Escolhendo a cor da linha 
plt.plot(faturamento_ano.index, faturamento_ano.values, marker='o', linestyle='-', color='slateblue')
# Adicionando títulos e rótulos
plt.title(f'Faturamento Bruto Anual da Artista: {top_artista}', fontsize=20, fontweight='bold')
plt.xlabel('Ano da Turnê', fontsize=12, fontweight='bold')
plt.ylabel('Faturamento Bruto Total (em milhões de $)', fontsize=12, fontweight='bold')
plt.grid(True, linestyle='--', alpha=0.6)
plt.xticks(faturamento_ano.index.astype(int))
# Formatando números
plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'${x/1e6:.1f}M'))
plt.tight_layout()
# Salvando o gráfico em .png 
plt.savefig(pasta_volume / 'Q4.png')
plt.close()

# Q5. Gráfico de colunas demonstrando as 5 artistas com mais shows

# Agrupando por artista, somando shows e ordenando os artistas
shows_artista = df_limpo.groupby('Artist')['Shows'].sum().sort_values(ascending=False).head(5)
# Criando a figura para o gráfico de barras
plt.figure(figsize=(12, 9))
# Customizando o gráfico
shows_artista.plot(kind='bar', color='slateblue')
# Título do Gráfico
plt.title("5 artistas com mais Shows", fontsize=20, fontweight='bold')
# Nomes dos eixos X e Y
plt.xlabel("Artistas", fontsize=12, fontweight='bold')
plt.ylabel("Número Total de Shows", fontsize=12, fontweight='bold')
# Rotacionando os nomes dos apps:
plt.xticks(rotation=45, ha='right')
# Salvando o gráfico em .png
plt.savefig(pasta_volume / 'Q5.png')
plt.close()
