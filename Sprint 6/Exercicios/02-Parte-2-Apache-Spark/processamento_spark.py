# Importando as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, rand, floor, element_at
import pathlib

# Pasta para receber arquivos 
pasta_respostas = pathlib.Path('respostas')
# Evita erros se a pasta já existir
pasta_respostas.mkdir(exist_ok=True)

# Iniciando o Spark
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Exercício Spark") \
    .getOrCreate()

# Leitura de Arquivo com nomes
print(
f"""{50*'='}    
                Lendo Arquvo de Nomes...
{50*'='}
    """
)

# Lendo o arquivos com nomes e pulando a primeira linha - Título do Arquivo
df_nomes_leitura = spark.read.csv('../01-Parte-1-Geracao-e-massa-de-dados/respostas/nomes_aleatorios.txt', header=False, comment='=')
# Verificação do Schema
df_nomes_leitura.printSchema()

# Renomeando nome da coluna para "Nomes"
df_nomes= df_nomes_leitura.withColumnRenamed("_c0", "Nomes")
# Verificando Schema após a renomeação da coluna
df_nomes.printSchema()
# Mostrando os primeiros 5 nomes do DataFrame
df_nomes.show(5)

print(
f"""{50*'='}    
                   Escolaridade
{50*'='}
    """    
)

# Adição de Coluna "Escolaridade"
df_nomes = df_nomes.withColumn("Escolaridade",
    # Geração de números aleatórios para comparação e definição do valor da Escolaridade
    when(rand() < 0.33, lit("Fundamental"))
    .when(rand() < 0.66, lit("Medio"))
    # Se não for nenhum dos casos acima
    .otherwise(lit("Superior"))
)
df_nomes.show(10)

print(
f"""{50*'='}    
                   Escolaridade e País
{50*'='}
    """    
)
# Adição de Coluna "Pais"
paises_sul_americanos = [
    "Argentina", "Bolívia", "Brasil", "Chile", "Colômbia", "Equador",
    "Guiana", "Paraguai", "Peru", "Suriname", "Uruguai", "Venezuela", "Guiana Francesa"
]
# Definindo de forma aleatória os países sul americanos
paises_col = lit(paises_sul_americanos)
df_nomes = df_nomes.withColumn("Pais", element_at(paises_col, (floor(rand() * len(paises_sul_americanos)) + 1).cast("int")))
df_nomes.show(10)

print(
f"""{50*'='}    
                   Ano de Nascimento
{50*'='}
    """    
)

# Adição de Coluna "AnoNascimento"
df_nomes = df_nomes.withColumn("AnoNascimento", (1945 + floor(rand() * (2010 - 1945 + 1))).cast("int"))
df_nomes.select("Nomes", "AnoNascimento").show(10)

print(
f"""{80*'='}
                Pessoas Nascidas a Partir dos Anos 2000
{80*'='}
    """
)

# Selecionando Pessoas Nascidas a partir do ano 2000
df_select = df_nomes.filter(col("AnoNascimento") >= 2000)
df_select.select("Nomes", "AnoNascimento").show(10)

# Repetição do processo usando o Spark SQL 
print(
f"""{80*'='}
                Spark SQL - Pessoas Nascidas a Partir dos Anos 2000
{80*'='}
    """
)

# Criando uma view temporária para visualização
df_nomes.createOrReplaceTempView("pessoas")
# Selecionando Pessoas Nascidas a partir do ano 2000
df_sql_seculo = spark.sql("SELECT Nomes, AnoNascimento FROM pessoas WHERE AnoNascimento >= 2000")
df_sql_seculo.show(10)
# Salvando
caminho_saida_csv = pasta_respostas / "relatorio_seculo_csv"
df_sql_seculo.coalesce(1).write.csv(str(caminho_saida_csv), header=True, mode='overwrite')

# Contagem de Pessoas da Geração Millennials
total_millennials = df_nomes.filter((col("AnoNascimento") >= 1980) & (col("AnoNascimento") <= 1994)).count()
print(
    f"""{100*'='}
                Geração Millennials (Pessoas Nascidas entre 1980 e 1994)
{100*'='}
                Número total de pessoas da geração Millennials: {total_millennials}
    """
)

print(
f"""{100*'='}
                Spark SQL - Geração Millennials (Pessoas Nascidas entre 1980 e 1994) 
{100*'='}
        """
)

# Contagem de Pessoas da Geração Millennials - Spark SQL
df_sql_millennials = spark.sql("SELECT COUNT(*) as total FROM pessoas WHERE AnoNascimento BETWEEN 1980 AND 1994")
df_sql_millennials.show()
# Salvando
caminho_saida_csv = pasta_respostas / "relatorio_millenius_csv"
df_sql_millennials.coalesce(1).write.csv(str(caminho_saida_csv), header=True, mode='overwrite')


print(
f"""{50*'='}
                País e Gerações
{50*'='}
    """
)
# Quantidade de Pessoas por País para cada Geração 
query_geracoes = """
    SELECT
        Pais,
        CASE
            WHEN AnoNascimento BETWEEN 1944 AND 1964 THEN 'Baby Boomers'
            WHEN AnoNascimento BETWEEN 1965 AND 1979 THEN 'Geração X'
            WHEN AnoNascimento BETWEEN 1980 AND 1994 THEN 'Millennials'
            WHEN AnoNascimento BETWEEN 1995 AND 2010 THEN 'Geração Z'
            ELSE NULL
        END AS Geracao
    FROM pessoas
    WHERE AnoNascimento >= 1944
    GROUP BY Pais, Geracao
    ORDER BY Pais, Geracao
"""
df_geracoes = spark.sql(query_geracoes)
df_geracoes.show(df_geracoes.count(), truncate=False)

print("Salvando o resultado final em um arquivo CSV...")
caminho_saida_csv = pasta_respostas / "relatorio_geracoes_csv"
df_geracoes.coalesce(1).write.csv(str(caminho_saida_csv), header=True, mode='overwrite')

