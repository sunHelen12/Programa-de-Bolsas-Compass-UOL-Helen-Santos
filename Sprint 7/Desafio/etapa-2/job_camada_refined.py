# Script para execução do Job da Camada Refined no AWS Glue

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, concat, monotonically_increasing_id, split, lower, explode, year, month, dayofmonth, trim, when, current_timestamp, regexp_replace

# Leitura dos parâmetros do Job
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "TRUSTED_DB_MOVIE",
    "TRUSTED_DB_TMDB",
    "REFINED_DB",
    "REFINED_S3_PATH"
])

# Inicialização dos componentes do Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# Atribuição das variáveis a partir dos parâmetros
TRUSTED_DB_MOVIE = args['TRUSTED_DB_MOVIE']
TRUSTED_DB_TMDB = args['TRUSTED_DB_TMDB']
REFINED_DB = args['REFINED_DB']
REFINED_S3_PATH = args['REFINED_S3_PATH']

# Captura o timestamp para as colunas de auditoria
timestamp_execucao = current_timestamp()

def salvar_refined(df, nome_tabela):
    df_com_auditoria = df.withColumn("data_criacao", timestamp_execucao) \
                         .withColumn("data_atualizacao", timestamp_execucao)

    caminho_s3 = f"{REFINED_S3_PATH}{nome_tabela}/"
    logger.info(f"Salvando tabela '{nome_tabela}' em: {caminho_s3}")

    dynamic_frame_write = DynamicFrame.fromDF(df_com_auditoria, glueContext, f"df_{nome_tabela}")

    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame_write,
        connection_type="s3",
        connection_options={"path": caminho_s3, "database": REFINED_DB, "partitionKeys": []},
        format="parquet",
        transformation_ctx=f"write_{nome_tabela}"
    )
    logger.info(f"Tabela '{nome_tabela}' salva e catalogada com sucesso.")

logger.info("Iniciando leitura das tabelas da camada Trusted via Data Catalog...")
df_movies_trusted = glueContext.create_dynamic_frame.from_catalog(database=TRUSTED_DB_MOVIE, table_name="movies_dataset").toDF()
df_atores_trusted = glueContext.create_dynamic_frame.from_catalog(database=TRUSTED_DB_TMDB, table_name="atores_filmografia_trusted").toDF()
df_diretores_trusted = glueContext.create_dynamic_frame.from_catalog(database=TRUSTED_DB_TMDB, table_name="diretores_filmografia_trusted").toDF()
df_classificacao_trusted = glueContext.create_dynamic_frame.from_catalog(database=TRUSTED_DB_TMDB, table_name="terror_classificacao_trusted").toDF()
df_sk_trusted = glueContext.create_dynamic_frame.from_catalog(database=TRUSTED_DB_TMDB, table_name="stephen_king_trusted").toDF()
df_protagonista_trusted = glueContext.create_dynamic_frame.from_catalog(database=TRUSTED_DB_TMDB, table_name="protagonista_feminina_trusted").toDF()
df_terror_psicologico_trusted = glueContext.create_dynamic_frame.from_catalog(database=TRUSTED_DB_TMDB, table_name="terror_psicologico_misterio_trusted").toDF()
logger.info("Leitura da camada Trusted concluída.")

logger.info("Iniciando pré-processamento e normalização dos dados...")
# Padronização de IDs para inteiros
df_movies_trusted = df_movies_trusted.withColumn("id_filme", regexp_replace(col("id_filme"), "^tt", "").cast("int"))
df_atores_trusted = df_atores_trusted.withColumn("id_filme", col("filme_id").cast("int"))
df_diretores_trusted = df_diretores_trusted.withColumn("id_filme", col("filme_id").cast("int"))
df_protagonista_trusted = df_protagonista_trusted.withColumn("id_filme", col("filme_id").cast("int"))

# Normalização de títulos para o JOIN da dim_filme
df_movies_trusted = df_movies_trusted.withColumn("titulo_normalizado", regexp_replace(lower(col("titulo_principal")), "[^a-z0-9]", ""))
df_classificacao_trusted = df_classificacao_trusted.filter(col("filme_data_lancamento").isNotNull() & (col("filme_data_lancamento") != "")).withColumn("titulo_normalizado", regexp_replace(lower(col("filme_titulo")), "[^a-z0-9]", "")).withColumn("ano_lancamento", year(col("filme_data_lancamento")))
df_sk_trusted = df_sk_trusted.filter(col("filme_data_lancamento").isNotNull() & (col("filme_data_lancamento") != "")).withColumn("titulo_normalizado", regexp_replace(lower(col("filme_titulo")), "[^a-z0-9]", "")).withColumn("ano_lancamento", year(col("filme_data_lancamento"))).filter(col("fonte") == "stephen_king")
df_terror_psicologico_trusted = df_terror_psicologico_trusted.filter(col("filme_data_lancamento").isNotNull() & (col("filme_data_lancamento") != "")).withColumn("titulo_normalizado", regexp_replace(lower(col("filme_titulo")), "[^a-z0-9]", "")).withColumn("ano_lancamento", year(col("filme_data_lancamento")))
logger.info("Pré-processamento concluído.")

# Criando a dim_data
logger.info("Construindo a dim_data...")
meses_data = [(1, "Janeiro"), (2, "Fevereiro"), (3, "Março"), (4, "Abril"), (5, "Maio"), (6, "Junho"), (7, "Julho"), (8, "Agosto"), (9, "Setembro"), (10, "Outubro"), (11, "Novembro"), (12, "Dezembro")]
df_meses_lookup = spark.createDataFrame(meses_data, ["mes_numero", "mes_nome"])
df_datas = df_atores_trusted.select(col("filme_data_lancamento").alias("data_completa")).distinct().dropna()
dim_data_base = df_datas.withColumn("ano", year(col("data_completa"))).withColumn("mes_numero", month(col("data_completa"))).withColumn("dia", dayofmonth(col("data_completa")))
dim_data_enriquecida = dim_data_base.join(df_meses_lookup, on="mes_numero", how="left").withColumn("trimestre", when(col("mes_numero").isin([1, 2, 3]), lit("T1")).when(col("mes_numero").isin([4, 5, 6]), lit("T2")).when(col("mes_numero").isin([7, 8, 9]), lit("T3")).otherwise(lit("T4"))).withColumn("decada", concat(((col("ano") / 10).cast("integer") * 10).cast("string"), lit("s")))
dim_data = dim_data_enriquecida.withColumn("sk_data", col("ano") * 10000 + col("mes_numero") * 100 + col("dia")).select("sk_data", "data_completa", "ano", "mes_numero", "mes_nome", "dia", "trimestre", "decada").distinct()
salvar_refined(dim_data, "dim_data")

# Criando dim_diretor
logger.info("Construindo a dim_diretor...")
dim_diretor = df_diretores_trusted.select(col("diretor_id").alias("id_diretor"), col("diretor").alias("nome_diretor")).distinct().withColumn("sk_diretor", monotonically_increasing_id())
salvar_refined(dim_diretor, "dim_diretor")

# Criando dim_artista
logger.info("Construindo a dim_artista...")
artistas_csv = df_movies_trusted.select("nome_artista", "genero_artista", "ano_nascimento", "ano_falecimento", regexp_replace(col("titulos_mais_conhecidos"), "tt", "").alias("titulos_mais_conhecidos")).distinct()
atores_json = df_atores_trusted.select(col("ator_id").alias("id_artista"), col("ator").alias("nome_artista")).distinct()
dim_artista = atores_json.join(artistas_csv, on="nome_artista", how="left").withColumn("sk_artista", monotonically_increasing_id())
salvar_refined(dim_artista, "dim_artista")

# Criando dim_genero
logger.info("Construindo a dim_genero...")
dim_genero = df_movies_trusted.select(explode(split(col("genero"), ",")).alias("nome_genero")).select(trim(col("nome_genero")).alias("nome_genero")).distinct().dropna()
dim_genero = dim_genero.withColumn("sk_genero", monotonically_increasing_id())
salvar_refined(dim_genero, "dim_genero")

# Criando a dim_filme
logger.info("Construindo a dim_filme...")
dim_filme_base = df_movies_trusted.select("id_filme", "titulo_principal", "titulo_original", "ano_lancamento", "tempo_minutos", "titulo_normalizado").distinct()
dim_filme_temp = dim_filme_base.join(df_classificacao_trusted.select("titulo_normalizado", "ano_lancamento", col("classificacao").alias("classificacao_indicativa")), on=["titulo_normalizado", "ano_lancamento"], how="left")
dim_filme_temp = dim_filme_temp.join(df_sk_trusted.select("titulo_normalizado", "ano_lancamento").withColumn("flag_sk", lit(True)), on=["titulo_normalizado", "ano_lancamento"], how="left")
dim_filme_temp = dim_filme_temp.join(df_terror_psicologico_trusted.select("titulo_normalizado", "ano_lancamento", col("categoria").alias("categoria_terror")), on=["titulo_normalizado", "ano_lancamento"], how="left")
dim_filme = dim_filme_temp.withColumn("baseado_em_stephen_king", when(col("flag_sk").isNotNull(), True).otherwise(False)).drop("flag_sk", "titulo_normalizado").withColumn("sk_filme", monotonically_increasing_id())
salvar_refined(dim_filme, "dim_filme")

# Criando ponte_filme_genero
logger.info("Construindo a ponte_filme_genero...")
df_generos_exploded = df_movies_trusted.select("id_filme", explode(split(col("genero"), ",")).alias("nome_genero")).select("id_filme", trim(col("nome_genero")).alias("nome_genero"))
ponte_temp1 = df_generos_exploded.join(dim_filme, on="id_filme", how="inner")
ponte_temp2 = ponte_temp1.join(dim_genero, on="nome_genero", how="inner")
ponte_filme_genero = ponte_temp2.select("sk_filme", "sk_genero")
salvar_refined(ponte_filme_genero, "ponte_filme_genero")

# Criando a fato_participacao
logger.info("Construindo a fato_participacao...")

df_filmes_protagonistas_fem = df_protagonista_trusted.select(col("id_filme").alias("id_filme_protagonista")).distinct()
part_temp1 = df_movies_trusted.select("id_filme", "nome_artista", "personagem", "genero_artista").dropna(subset=["nome_artista"])
part_temp2 = part_temp1.join(df_filmes_protagonistas_fem, part_temp1.id_filme == df_filmes_protagonistas_fem.id_filme_protagonista, how="left")
part_temp3 = part_temp2.withColumn(
    "is_protagonista",
    (col("personagem").isNotNull()) | (col("id_filme_protagonista").isNotNull() & (col("genero_artista") == 'Feminino'))
)
part_temp4 = part_temp3.join(dim_filme, on="id_filme", how="inner")
dim_artista_para_join = dim_artista.select("sk_artista", "nome_artista")
part_temp5 = part_temp4.join(dim_artista_para_join, on="nome_artista", how="inner")
fato_participacao = part_temp5.select(
    "sk_filme",
    "sk_artista",
    "personagem",
    "is_protagonista",
    col("genero_artista").alias("genero_artista_participacao")
)

salvar_refined(fato_participacao, "fato_participacao")

# Criando a fato_filme
logger.info("Construindo a fato_filme...")
metricas_atores = df_atores_trusted.groupBy("id_filme").agg({"filme_orcamento": "first", "filme_receita": "first"}).withColumnRenamed("first(filme_orcamento)", "orcamento").withColumnRenamed("first(filme_receita)", "receita")
fato_filme = df_movies_trusted.select("id_filme", "nota_media", "numero_votos") \
    .join(metricas_atores, on="id_filme", how="left") \
    .join(df_diretores_trusted.select("id_filme", "diretor", "filme_data_lancamento"), on="id_filme", how="left") \
    .join(dim_filme.select("id_filme", "sk_filme"), on="id_filme", how="inner") \
    .join(dim_diretor.select("nome_diretor", "sk_diretor"), col("diretor") == col("nome_diretor"), how="left") \
    .join(dim_data.select("data_completa", "sk_data"), col("filme_data_lancamento") == col("data_completa"), how="left") \
    .withColumn("lucro", col("receita") - col("orcamento")) \
    .select("sk_filme", "sk_diretor", col("sk_data").alias("sk_data_lancamento"), "nota_media", "numero_votos", "orcamento", "receita", "lucro")
salvar_refined(fato_filme, "fato_filme")

job.commit()
logger.info("Job da camada Refined finalizado com sucesso!")