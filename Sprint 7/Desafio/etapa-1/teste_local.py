# Script para execução local do Job da Camada Refined com PySpark
import os
import shutil
from dotenv import load_dotenv
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, monotonically_increasing_id, split, lower, explode, year, month, dayofmonth, trim, when, current_timestamp, regexp_replace

# Carregamento de variáveis de ambiente
load_dotenv()
print("Variáveis de ambiente do arquivo .env carregadas.")

# Leitura das variáveis
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')
aws_region = os.getenv('AWS_REGION')
s3_bucket_name = os.getenv('S3_BUCKET')

# Validação para garantir que todas as variáveis foram carregadas 
if not all([aws_access_key_id, aws_secret_access_key, aws_session_token, aws_region, s3_bucket_name]):
    raise ValueError("Uma ou mais variáveis de ambiente necessárias (AWS_*, S3_BUCKET) não foram definidas!")

# Criando SparkSession Local
spark = SparkSession.builder \
    .appName("JobRefinedLocalBoto3") \
    .master("local[*]") \
    .getOrCreate()

print("SparkSession iniciada.")

# Criando cliente boto3
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name=aws_region
)

# Criando pasta temporária para download
temp_dir = "arquivos_temporarios"
os.makedirs(temp_dir, exist_ok=True)

print(f"Arquivos temporários serão baixados em: {temp_dir}")

# Função para baixar arquivos parquet do S3
def baixar_parquet_s3(prefix):
    s3_objects = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=prefix)
    paths = []
    for obj in s3_objects.get('Contents', []):
        key = obj['Key']
        if key.endswith('.parquet'):
            local_path = os.path.join(temp_dir, os.path.basename(key))
            if not os.path.exists(local_path):
                print(f"Baixando {key} para {local_path}...")
                s3.download_file(s3_bucket_name, key, local_path)
            else:
                print(f"Arquivo {local_path} já existe, pulando download.")
            paths.append(local_path)
    return paths

# Definindo caminhos
TRUSTED_S3_PATH_MOVIES = f"Trusted/ARQUIVO_MOVIE/PARQUET/movies_dataset/"
TRUSTED_S3_PATH_TMDB = f"Trusted/TMDB/PARQUET/dados_filmografia/2025/09/26/"

# Baixando e validando todas as tabelas necessárias
df_movies_path = baixar_parquet_s3(f'{TRUSTED_S3_PATH_MOVIES}')
df_atores_path = baixar_parquet_s3(f'{TRUSTED_S3_PATH_TMDB}atores_filmografia_trusted/')
df_diretores_path = baixar_parquet_s3(f'{TRUSTED_S3_PATH_TMDB}diretores_filmografia_trusted/')
df_classificacao_path = baixar_parquet_s3(f'{TRUSTED_S3_PATH_TMDB}terror_classificacao_trusted/')
df_sk_path = baixar_parquet_s3(f'{TRUSTED_S3_PATH_TMDB}stephen_king_trusted/')
df_protagonista_path = baixar_parquet_s3(f'{TRUSTED_S3_PATH_TMDB}protagonista_feminina_trusted/')
df_terror_psicologico_path = baixar_parquet_s3(f'{TRUSTED_S3_PATH_TMDB}terror_psicologico_misterio_trusted/')

# Leitura de arquivos
df_movies_trusted = spark.read.parquet(*df_movies_path)
df_atores_trusted = spark.read.parquet(*df_atores_path)
df_diretores_trusted = spark.read.parquet(*df_diretores_path)
df_classificacao_trusted = spark.read.parquet(*df_classificacao_path)
df_sk_trusted = spark.read.parquet(*df_sk_path)
df_protagonista_trusted = spark.read.parquet(*df_protagonista_path)
df_terror_psicologico_trusted = spark.read.parquet(*df_terror_psicologico_path)

print("Leitura dos arquivos parquet concluída.")

# Pasta local para salvar resultados refined
REFINED_LOCAL_PATH = os.path.join("dados", "refined")
os.makedirs(REFINED_LOCAL_PATH, exist_ok=True)

# Função Auxiliar apra salvar localmente 
def salvar_refined_local (df, nome_tabela):
    caminho_local = os.path.join(REFINED_LOCAL_PATH, nome_tabela)
    print(f"Salvando tabela '{nome_tabela}' localmente em: {caminho_local}")
    df.write.mode("overwrite").parquet(caminho_local)
    print(f"Tabela '{nome_tabela}' salva com sucesso.")

# Capturando timestamp e para função de salvar para auditoria
timestamp_execucao = current_timestamp()

# Tratando id_filme da tabela movies
df_movies_trusted = df_movies_trusted.withColumn("id_filme_original", col("id_filme")) \
    .withColumn("id_filme", regexp_replace(col("id_filme"), "^tt", "")) \
    .withColumn("id_filme", col("id_filme").cast("int"))

# Garantindo que todos os IDs sejam inteiros 
df_atores_trusted = df_atores_trusted.withColumn("id_filme", col("filme_id").cast("int"))
df_diretores_trusted = df_diretores_trusted.withColumn("id_filme", col("filme_id").cast("int"))
df_classificacao_trusted = df_classificacao_trusted.withColumn("id_filme", col("filme_id").cast("int"))
df_sk_trusted = df_sk_trusted.withColumn("id_filme", col("filme_id").cast("int"))
df_protagonista_trusted = df_protagonista_trusted.withColumn("id_filme", col("filme_id").cast("int"))

# Normalizando titulo em movies
df_movies_trusted = df_movies_trusted.withColumn(
    "titulo_normalizado",
    regexp_replace(lower(col("titulo_principal")), "[^a-z0-9]", "")
)

# Normalizando título em classificações
df_classificacao_trusted = df_classificacao_trusted \
    .filter(col("filme_data_lancamento").isNotNull() & (col("filme_data_lancamento") != "")) \
    .withColumn(
        "titulo_normalizado",
        regexp_replace(lower(col("filme_titulo")), "[^a-z0-9]", "")
    ).withColumn(
        "ano_lancamento", year(col("filme_data_lancamento"))
    ).withColumnRenamed("classificacao", "classificacao_indicativa")

# Normalizando título em stephen king
df_sk_trusted = df_sk_trusted \
    .filter(col("filme_data_lancamento").isNotNull() & (col("filme_data_lancamento") != "")) \
    .withColumn(
        "titulo_normalizado",
        regexp_replace(lower(col("filme_titulo")), "[^a-z0-9]", "")
    ).withColumn(
        "ano_lancamento", year(col("filme_data_lancamento"))
    ).filter(col("fonte") == "stephen_king")

# Normalizand terror psicológico e mistério
df_terror_psicologico_trusted = df_terror_psicologico_trusted \
    .filter(col("filme_data_lancamento").isNotNull() & (col("filme_data_lancamento") != "")) \
    .withColumn(
        "titulo_normalizado",
        regexp_replace(lower(col("filme_titulo")), "[^a-z0-9]", "")
    ).withColumn(
        "ano_lancamento", year(col("filme_data_lancamento"))
    )

# Criando a dim_data
print("Construindo a dim_data...")
meses_data = [
    (1, "Janeiro"), (2, "Fevereiro"), (3, "Março"), (4, "Abril"),
    (5, "Maio"), (6, "Junho"), (7, "Julho"), (8, "Agosto"),
    (9, "Setembro"), (10, "Outubro"), (11, "Novembro"), (12, "Dezembro")
]
df_meses_lookup = spark.createDataFrame(meses_data, ["mes_numero", "mes_nome"])
df_datas = df_atores_trusted.select(col("filme_data_lancamento").alias("data_completa")).distinct().dropna()
dim_data_base = df_datas.withColumn("ano", year(col("data_completa"))) \
                        .withColumn("mes_numero", month(col("data_completa"))) \
                        .withColumn("dia", dayofmonth(col("data_completa")))
dim_data_enriquecida = dim_data_base.join(df_meses_lookup, on="mes_numero", how="left") \
                                    .withColumn("trimestre", when(col("mes_numero").isin([1, 2, 3]), lit("T1")).when(col("mes_numero").isin([4, 5, 6]), lit("T2")).when(col("mes_numero").isin([7, 8, 9]), lit("T3")).otherwise(lit("T4"))) \
                                    .withColumn("decada", concat(((col("ano") / 10).cast("integer") * 10).cast("string"), lit("s")))
dim_data = dim_data_enriquecida.withColumn("sk_data", col("ano") * 10000 + col("mes_numero") * 100 + col("dia")).select("sk_data", "data_completa", "ano", "mes_numero", "mes_nome", "dia", "trimestre", "decada").distinct()
salvar_refined_local(dim_data, "dim_data")

# Criando dim_diretor
print("Construindo a dim_diretor...")
dim_diretor = df_diretores_trusted.select(
    col("diretor_id").alias("id_diretor"),
    col("diretor").alias("nome_diretor")
).distinct().withColumn("sk_diretor", monotonically_increasing_id())
salvar_refined_local(dim_diretor, "dim_diretor")

# Criando dim_artista
print("Construindo a dim_artista...")
artistas_csv = df_movies_trusted.select(
    "nome_artista", 
    "genero_artista", 
    "ano_nascimento", 
    "ano_falecimento",
    regexp_replace(col("titulos_mais_conhecidos"), "tt", "").alias("titulos_mais_conhecidos")
).distinct()
atores_json = df_atores_trusted.select(col("ator_id").alias("id_artista"), col("ator").alias("nome_artista")).distinct()
dim_artista = atores_json.join(artistas_csv, on="nome_artista", how="left").withColumn("sk_artista", monotonically_increasing_id())
salvar_refined_local(dim_artista, "dim_artista")

# Criando dim_genero
print("Construindo a dim_genero...")
dim_genero = df_movies_trusted.select(explode(split(col("genero"), ",")).alias("nome_genero")).select(trim(col("nome_genero")).alias("nome_genero")).distinct().dropna()
dim_genero = dim_genero.withColumn("sk_genero", monotonically_increasing_id())
salvar_refined_local(dim_genero, "dim_genero")

# Criando a dim_filme
print("Construindo a dim_filme...")
dim_filme_base = df_movies_trusted.select(
    "id_filme", "titulo_principal", "titulo_original",
    "ano_lancamento", "tempo_minutos", "titulo_normalizado"
).distinct()

dim_filme_temp = dim_filme_base.join(
    df_classificacao_trusted.select("titulo_normalizado", "ano_lancamento", "classificacao_indicativa"),
    on=["titulo_normalizado", "ano_lancamento"], how="left"
)

dim_filme_temp = dim_filme_temp.join(
    df_sk_trusted.select("titulo_normalizado", "ano_lancamento").withColumn("flag_sk", lit(True)),
    on=["titulo_normalizado", "ano_lancamento"], how="left"
)

dim_filme_temp = dim_filme_temp.join(
    df_terror_psicologico_trusted.select("titulo_normalizado", "ano_lancamento", col("categoria").alias("categoria_terror")),
    on=["titulo_normalizado", "ano_lancamento"],
    how="left"
)

dim_filme = dim_filme_temp.withColumn(
    "baseado_em_stephen_king", when(col("flag_sk").isNotNull(), True).otherwise(False)
).drop("flag_sk", "titulo_normalizado").withColumn("sk_filme", monotonically_increasing_id())

salvar_refined_local(dim_filme, "dim_filme")

# Criando ponte_filme_genero
print("Construindo a ponte_filme_genero...")
df_generos_exploded = df_movies_trusted.select(
    col("id_filme"), 
    explode(split(col("genero"), ",")).alias("nome_genero")
).select("id_filme", trim(col("nome_genero")).alias("nome_genero"))
ponte_temp1 = df_generos_exploded.join(dim_filme, on="id_filme", how="inner")
ponte_temp2 = ponte_temp1.join(dim_genero, on="nome_genero", how="inner")
ponte_filme_genero = ponte_temp2.select("sk_filme", "sk_genero")
salvar_refined_local(ponte_filme_genero, "ponte_filme_genero")

# Criando a fato_participacao
print("Construindo a fato_participacao...")
df_filmes_protagonistas_fem = df_protagonista_trusted.select(
    col("id_filme").cast("int").alias("id_filme_protagonista")
).distinct()
part_temp1 = df_movies_trusted.select(
    col("id_filme"), 
    "nome_artista", 
    "personagem", 
    "genero_artista" 
).dropna(subset=["nome_artista"])

part_temp2 = part_temp1.join(df_filmes_protagonistas_fem, part_temp1.id_filme == df_filmes_protagonistas_fem.id_filme_protagonista, how="left")

part_temp2 = part_temp2.withColumn(
    "is_protagonista",
    (col("personagem").isNotNull()) |
    (col("id_filme_protagonista").isNotNull() & (col("genero_artista") == 'Feminino')) 
)
part_temp3 = part_temp2.join(dim_filme, on="id_filme", how="inner")

dim_artista_para_join = dim_artista.select("nome_artista", "sk_artista")

part_temp4 = part_temp3.join(dim_artista_para_join, on="nome_artista", how="inner") 

fato_participacao = part_temp4.select(
    "sk_filme",
    "sk_artista",
    "personagem",
    "is_protagonista",
    col("genero_artista").alias("genero_protagonista")
)
salvar_refined_local(fato_participacao, "fato_participacao")

# Criando a fato_filme 
print("Criando a fato_filme...")
metricas_atores = df_atores_trusted.groupBy("id_filme").agg(
    {"filme_orcamento": "first", "filme_receita": "first"}
).withColumnRenamed("first(filme_orcamento)", "orcamento") \
 .withColumnRenamed("first(filme_receita)", "receita")

fato_filme = df_movies_trusted.select("id_filme", "nota_media", "numero_votos") \
    .join(metricas_atores, on="id_filme", how="left") \
    .join(df_diretores_trusted.select("id_filme", "diretor", "filme_data_lancamento"), on="id_filme", how="left") \
    .join(dim_filme.select("id_filme", "sk_filme"), on="id_filme", how="inner") \
    .join(dim_diretor.select("nome_diretor", "sk_diretor"), col("diretor") == col("nome_diretor"), how="left") \
    .join(dim_data.select("data_completa", "sk_data"), col("filme_data_lancamento") == col("data_completa"), how="left") \
    .withColumn("lucro", col("receita") - col("orcamento")) \
    .select(
        col("sk_filme"),
        col("sk_diretor"),
        col("sk_data").alias("sk_data_lancamento"),
        col("nota_media"),
        col("numero_votos"),
        col("orcamento"),
        col("receita"),
        col("lucro")
    )

salvar_refined_local(fato_filme, "fato_filme")

# Removendo arquivos temporários do S3
shutil.rmtree(temp_dir)
print("Arquivos temporários apagados.")

spark.stop()
print("Job Refined local finalizado com sucesso!")