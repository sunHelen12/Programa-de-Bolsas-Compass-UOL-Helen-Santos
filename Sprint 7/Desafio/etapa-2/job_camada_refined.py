# Script para execução do Job da Camada Refined no AWS Glue 
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, first, concat, row_number, split, lower, explode, year, month, dayofmonth, trim, when, regexp_replace, coalesce, upper, regexp_extract

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

def salvar_refined(df, nome_tabela):
    caminho_s3 = f"{REFINED_S3_PATH}{nome_tabela}/"
    logger.info(f"Salvando tabela '{nome_tabela}' em: {caminho_s3}")

    dynamic_frame_write = DynamicFrame.fromDF(df, glueContext, f"df_{nome_tabela}")

    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame_write,
        connection_type="s3",
        connection_options={"path": caminho_s3, "database": REFINED_DB, "partitionKeys": []},
        format="parquet",
        transformation_ctx=f"write_{nome_tabela}"
    )
    logger.info(f"Tabela '{nome_tabela}' salva e catalogada com sucesso.")

# Função para validar qualidade dos dados
def validar_tabela(df, nome_tabela, chave_primaria=None):
    """Função para validar qualidade dos dados"""
    total_registros = df.count()
    logger.info(f"Tabela {nome_tabela}: {total_registros} registros")
    
    if chave_primaria:
        if isinstance(chave_primaria, list):
            duplicatas = df.groupBy(*chave_primaria).count().filter(col("count") > 1)
        else:
            duplicatas = df.groupBy(chave_primaria).count().filter(col("count") > 1)
        
        total_duplicatas = duplicatas.count()
        if total_duplicatas > 0:
            logger.warn(f"Tabela {nome_tabela}: {total_duplicatas} chaves primárias duplicadas")
            # Remover duplicatas mantendo o primeiro registro
            if isinstance(chave_primaria, list):
                window = Window.partitionBy(*chave_primaria).orderBy(chave_primaria[0])
            else:
                window = Window.partitionBy(chave_primaria).orderBy(chave_primaria)
            
            df = df.withColumn("row_num", row_number().over(window)) \
                  .filter(col("row_num") == 1) \
                  .drop("row_num")
            logger.info(f"Duplicatas removidas da tabela {nome_tabela}")
        else:
            logger.info(f"Tabela {nome_tabela}: Sem duplicatas na chave primária")
    
    return df

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

#  Função melhorada para normalização de títulos
def normalizar_titulo(titulo_col):
    """Normaliza títulos removendo caracteres especiais, artigos e espaços extras"""
    return regexp_replace(
        regexp_replace(
            lower(trim(titulo_col)),
            r"\b(the|a|an|and|or|but)\b|['\":;,.!?\-]",  # Remove artigos e pontuação
            ""
        ),
        r"\s+",  # Remove espaços múltiplos
        ""
    )

# Padronização de IDs para inteiros 
df_movies_trusted = df_movies_trusted.withColumn("id_filme", regexp_replace(col("id_filme"), "^tt", "").cast("int"))
df_atores_trusted = df_atores_trusted.withColumn("id_filme", col("filme_id").cast("int"))
df_diretores_trusted = df_diretores_trusted.withColumn("id_filme", col("filme_id").cast("int"))
df_classificacao_trusted = df_classificacao_trusted.withColumn("id_filme", col("filme_id").cast("int"))
df_sk_trusted = df_sk_trusted.withColumn("id_filme", col("filme_id").cast("int"))
df_protagonista_trusted = df_protagonista_trusted.withColumn("id_filme", col("filme_id").cast("int"))
df_terror_psicologico_trusted = df_terror_psicologico_trusted.withColumn("id_filme", col("filme_id").cast("int"))

# Normalização de títulos melhorada
df_movies_trusted = df_movies_trusted.withColumn("titulo_normalizado", normalizar_titulo(col("titulo_principal")))

# Processar classificação indicativa de forma mais precisa
df_classificacao_trusted = df_classificacao_trusted.withColumn(
    "classificacao_indicativa",
    when(col("classificacao") == "terror_pg13", "PG-13")
    .when(col("classificacao") == "terror_r", "R")  
    .when(lower(col("classificacao")).contains("pg"), "PG-13")
    .when(lower(col("classificacao")).contains("r"), "R")
    .when(lower(col("classificacao")).contains("g"), "G")
    .otherwise("Não Classificado")
)

# Identificação de Stephen King mais precisa
df_sk_trusted = df_sk_trusted.withColumn(
    "stephen_king_confirmado",
    when(
        (col("fonte") == "stephen_king") | 
        (lower(col("filme_titulo")).contains("stephen king")) |
        (lower(col("filme_titulo")).contains("shining")) |
        (lower(col("filme_titulo")).contains("it chapter")) |
        (lower(col("filme_titulo")).contains("carrie")) |
        (lower(col("filme_titulo")).contains("misery")) |
        (lower(col("filme_titulo")).contains("pet sematary")) |
        (lower(col("filme_titulo")).contains("green mile")) |
        (lower(col("filme_titulo")).contains("shawshank")),
        True
    ).otherwise(False)
)

#  Processar categoria de terror psicológico
df_terror_psicologico_trusted = df_terror_psicologico_trusted.withColumn(
    "categoria_terror",
    when(col("categoria") == "terror_psicologico", "Terror Psicológico")
    .when(col("categoria").contains("psicologico"), "Terror Psicológico")
    .when(col("categoria").contains("misterio"), "Mistério")
    .otherwise("Outros")
)

logger.info("Pré-processamento concluído.")

# Unificação correta dos IDs de filme com estratégia melhorada
logger.info("Construindo dim_filme unificada...")

# Coletar filmes da fonte MOVIES
filmes_movies = df_movies_trusted.select(
    col("id_filme").alias("id_filme_movies"),
    col("titulo_principal"),
    col("titulo_original"), 
    col("ano_lancamento"),
    col("tempo_minutos"),
    col("nota_media").alias("nota_media_movies"),
    col("numero_votos").alias("numero_votos_movies"),
    col("genero"),
    col("titulo_normalizado")
).distinct()

# Coletar filmes da fonte TMDB de forma unificada
def coletar_filmes_tmdb(df_source, id_col, titulo_col):
    """Função auxiliar para coletar filmes de fontes TMDB"""
    return df_source.select(
        col(id_col).alias("id_filme_tmdb"),
        col(titulo_col).alias("titulo_principal"),
        col(titulo_col).alias("titulo_original"),
        year(col("filme_data_lancamento")).alias("ano_lancamento"),
        col("filme_nota_media").alias("nota_media_tmdb"),
        lit(None).alias("filme_orcamento"),
        lit(None).alias("filme_receita"),
        normalizar_titulo(col(titulo_col)).alias("titulo_normalizado")
    ).filter(col("titulo_principal").isNotNull())

# Coletar de todas as fontes TMDB
filmes_tmdb_sources = [
    coletar_filmes_tmdb(df_atores_trusted, "filme_id", "filme_titulo"),
    coletar_filmes_tmdb(df_diretores_trusted, "filme_id", "filme_titulo"),
    coletar_filmes_tmdb(df_classificacao_trusted, "filme_id", "filme_titulo"),
    coletar_filmes_tmdb(df_sk_trusted, "filme_id", "filme_titulo"),
    coletar_filmes_tmdb(df_terror_psicologico_trusted, "filme_id", "filme_titulo"),
    coletar_filmes_tmdb(df_protagonista_trusted, "filme_id", "filme_titulo")
]

# Unificar TMDB
filmes_tmdb = filmes_tmdb_sources[0]
for source in filmes_tmdb_sources[1:]:
    filmes_tmdb = filmes_tmdb.union(source)

# Agregar TMDB removendo duplicatas
filmes_tmdb_agg = filmes_tmdb.groupBy("id_filme_tmdb", "titulo_normalizado", "ano_lancamento").agg(
    first("titulo_principal", ignorenulls=True).alias("titulo_principal_tmdb"),
    first("titulo_original", ignorenulls=True).alias("titulo_original_tmdb"),
    first("nota_media_tmdb", ignorenulls=True).alias("nota_media_tmdb"),
    first("filme_orcamento", ignorenulls=True).alias("orcamento"),
    first("filme_receita", ignorenulls=True).alias("receita")
).filter(col("titulo_principal_tmdb").isNotNull())

# JOIN MOVIES-TMDB com estratégia de fallback
filmes_unificados = filmes_movies.alias("movies").join(
    filmes_tmdb_agg.alias("tmdb"),
    (col("movies.titulo_normalizado") == col("tmdb.titulo_normalizado")) & 
    (col("movies.ano_lancamento") == col("tmdb.ano_lancamento")),
    how="full"
)

# Seleção e consolidação explícita
filmes_unificados_clean = filmes_unificados.select(
    col("movies.id_filme_movies").alias("id_filme_movies"),
    col("tmdb.id_filme_tmdb").alias("id_filme_tmdb"),
    col("movies.titulo_principal").alias("titulo_principal_movies"),
    col("tmdb.titulo_principal_tmdb").alias("titulo_principal_tmdb"),
    col("movies.titulo_original").alias("titulo_original_movies"), 
    col("tmdb.titulo_original_tmdb").alias("titulo_original_tmdb"),
    coalesce(col("movies.ano_lancamento"), col("tmdb.ano_lancamento")).alias("ano_lancamento"),
    col("movies.tempo_minutos").alias("tempo_minutos"),
    col("movies.nota_media_movies").alias("nota_media_movies"),
    col("tmdb.nota_media_tmdb").alias("nota_media_tmdb"),
    col("movies.numero_votos_movies").alias("numero_votos_movies"),
    col("tmdb.orcamento").alias("orcamento"),
    col("tmdb.receita").alias("receita"),
    col("movies.genero").alias("genero")
)

# Criar SK único e consolidar dados
window_sk = Window.orderBy(coalesce(col("id_filme_tmdb"), col("id_filme_movies")))
dim_filme_corrigida = filmes_unificados_clean.withColumn("sk_filme", row_number().over(window_sk)) \
    .withColumn("titulo_principal", 
               coalesce(col("titulo_principal_movies"), col("titulo_principal_tmdb"))) \
    .withColumn("titulo_original", 
               coalesce(col("titulo_original_movies"), col("titulo_original_tmdb"))) \
    .withColumn("nota_media",
               coalesce(col("nota_media_movies"), col("nota_media_tmdb"))) \
    .withColumn("numero_votos",
               coalesce(col("numero_votos_movies"), lit(0))) \
    .select(
        "sk_filme",
        "id_filme_movies",
        "id_filme_tmdb", 
        "titulo_principal",
        "titulo_original", 
        "ano_lancamento",
        "tempo_minutos",
        "nota_media",
        "numero_votos",
        "orcamento",
        "receita",
        "genero"
    ).filter(col("titulo_principal").isNotNull())  # CORREÇÃO: Remover registros sem título

# Enriquecer dim_filme com metadados de forma mais robusta
logger.info("Enriquecendo dim_filme com metadados...")

# Adicionar classificação indicativa
dim_filme_enriquecida = dim_filme_corrigida.join(
    df_classificacao_trusted.select(
        col("filme_id").alias("id_filme_class"),
        "classificacao_indicativa"
    ).distinct(),
    col("id_filme_tmdb") == col("id_filme_class"),
    how="left"
).drop("id_filme_class")

# Adicionar Stephen King
dim_filme_enriquecida = dim_filme_enriquecida.join(
    df_sk_trusted.select(
        col("filme_id").alias("id_filme_sk"),
        "stephen_king_confirmado"
    ).distinct(),
    col("id_filme_tmdb") == col("id_filme_sk"),
    how="left"
).withColumn("baseado_em_stephen_king", 
             coalesce(col("stephen_king_confirmado"), lit(False))).drop("id_filme_sk", "stephen_king_confirmado")

# Adicionar terror psicológico
dim_filme_enriquecida = dim_filme_enriquecida.join(
    df_terror_psicologico_trusted.select(
        col("filme_id").alias("id_filme_terror"),
        "categoria_terror"
    ).distinct(),
    col("id_filme_tmdb") == col("id_filme_terror"), 
    how="left"
).drop("id_filme_terror")

# Adicionar protagonistas femininas por década
dim_filme_enriquecida = dim_filme_enriquecida.join(
    df_protagonista_trusted.select(
        col("filme_id").alias("id_filme_protagonista"),
        col("decada").alias("decada_protagonista_feminina")
    ).distinct(),
    col("id_filme_tmdb") == col("id_filme_protagonista"),
    how="left"
).drop("id_filme_protagonista")

# Dim_filme final
dim_filme_final = dim_filme_enriquecida.select(
    "sk_filme",
    "id_filme_movies", 
    "id_filme_tmdb",
    "titulo_principal",
    "titulo_original",
    "ano_lancamento", 
    "tempo_minutos",
    "nota_media",           
    "numero_votos",        
    "orcamento",            
    "receita",             
    "classificacao_indicativa",
    "categoria_terror",
    "baseado_em_stephen_king",
    "decada_protagonista_feminina"
)

dim_filme_final = validar_tabela(dim_filme_final, "dim_filme", "sk_filme")
salvar_refined(dim_filme_final, "dim_filme")

# Criando a dim_data
logger.info("Construindo a dim_data...")
meses_data = [(1, "Janeiro"), (2, "Fevereiro"), (3, "Março"), (4, "Abril"), (5, "Maio"), (6, "Junho"), (7, "Julho"), (8, "Agosto"), (9, "Setembro"), (10, "Outubro"), (11, "Novembro"), (12, "Dezembro")]
df_meses_lookup = spark.createDataFrame(meses_data, ["mes_numero", "mes_nome"])

# Coletar datas de todas as fontes para dim_data
df_datas_atores = df_atores_trusted.select(col("filme_data_lancamento").alias("data_completa")).distinct().dropna()
df_datas_diretores = df_diretores_trusted.select(col("filme_data_lancamento").alias("data_completa")).distinct().dropna()
df_datas_classificacao = df_classificacao_trusted.select(col("filme_data_lancamento").alias("data_completa")).distinct().dropna()
df_datas_sk = df_sk_trusted.select(col("filme_data_lancamento").alias("data_completa")).distinct().dropna()
df_datas_protagonista = df_protagonista_trusted.select(col("filme_data_lancamento").alias("data_completa")).distinct().dropna()
df_datas_terror_psico = df_terror_psicologico_trusted.select(col("filme_data_lancamento").alias("data_completa")).distinct().dropna()

# Criar data a partir do ano_lancamento
df_datas_movies = df_movies_trusted.select(
    concat(col("ano_lancamento"), lit("-01-01")).cast("date").alias("data_completa")
).distinct().dropna()

# Unificar todas as datas
df_datas_unificadas = df_datas_atores.union(df_datas_diretores).union(df_datas_classificacao)\
    .union(df_datas_sk).union(df_datas_protagonista).union(df_datas_terror_psico)\
    .union(df_datas_movies).distinct()

dim_data_base = df_datas_unificadas.withColumn("ano", year(col("data_completa"))).withColumn("mes_numero", month(col("data_completa"))).withColumn("dia", dayofmonth(col("data_completa")))
dim_data_enriquecida = dim_data_base.join(df_meses_lookup, on="mes_numero", how="left").withColumn("trimestre", when(col("mes_numero").isin([1, 2, 3]), lit("T1")).when(col("mes_numero").isin([4, 5, 6]), lit("T2")).when(col("mes_numero").isin([7, 8, 9]), lit("T3")).otherwise(lit("T4"))).withColumn("decada", concat(((col("ano") / 10).cast("integer") * 10).cast("string"), lit("s")))
dim_data = dim_data_enriquecida.withColumn("sk_data", col("ano") * 10000 + col("mes_numero") * 100 + col("dia")).select("sk_data", "data_completa", "ano", "mes_numero", "mes_nome", "dia", "trimestre", "decada").distinct()
dim_data = validar_tabela(dim_data, "dim_data", "sk_data")
salvar_refined(dim_data, "dim_data")

# Criando dim_diretor
logger.info("Construindo a dim_diretor...")
window_diretor = Window.orderBy(col("id_diretor"))
dim_diretor = df_diretores_trusted.select(col("diretor_id").alias("id_diretor"), col("diretor").alias("nome_diretor")) \
    .distinct() \
    .withColumn("sk_diretor", row_number().over(window_diretor)) \
    .select("id_diretor", "nome_diretor", "sk_diretor") 
dim_diretor = validar_tabela(dim_diretor, "dim_diretor", "sk_diretor")
salvar_refined(dim_diretor, "dim_diretor")

# Criando dim_artista de forma mais precisa
logger.info("Construindo a dim_artista...")

# Coletar atores de TMDB
atores_tmdb = df_atores_trusted.select(
    col("ator_id").alias("id_artista"), 
    col("ator").alias("nome_artista"),
    lit("Ator").alias("tipo_artista"),
    lit("Masculino").alias("genero_artista_padrao")
).distinct()

# Coletar artistas de movies de forma mais completa
artistas_movies = df_movies_trusted.select(
    col("nome_artista"), 
    col("genero_artista"),
    col("ano_nascimento"), 
    col("ano_falecimento"), 
    regexp_replace(col("titulos_mais_conhecidos"), "tt", "").alias("titulos_mais_conhecidos"),
    col("profissao").alias("tipo_artista")
).filter(col("nome_artista").isNotNull()).distinct()

# Combinar fontes
artistas_unificados = atores_tmdb.unionByName(
    artistas_movies.select(
        lit(None).alias("id_artista"),
        "nome_artista",
        "tipo_artista", 
        col("genero_artista").alias("genero_artista_padrao"),
        lit(None).alias("ano_nascimento"),
        lit(None).alias("ano_falecimento"),
        lit(None).alias("titulos_mais_conhecidos")
    ),
    allowMissingColumns=True
)

window_artista = Window.orderBy("nome_artista")
dim_artista = artistas_unificados.withColumn("sk_artista", row_number().over(window_artista)) \
    .select(
        "sk_artista",
        "id_artista",
        "nome_artista", 
        "tipo_artista",
        "genero_artista_padrao",
        "ano_nascimento", 
        "ano_falecimento", 
        "titulos_mais_conhecidos"
    )

dim_artista = validar_tabela(dim_artista, "dim_artista", "sk_artista")
salvar_refined(dim_artista, "dim_artista")

# Criando dim_genero
logger.info("Construindo a dim_genero...")
window_genero = Window.orderBy(col("nome_genero"))
dim_genero = df_movies_trusted.select(explode(split(col("genero"), ",")).alias("nome_genero")) \
    .select(trim(col("nome_genero")).alias("nome_genero")) \
    .distinct() \
    .dropna() \
    .withColumn("sk_genero", row_number().over(window_genero)) \
    .select("nome_genero", "sk_genero") 
dim_genero = validar_tabela(dim_genero, "dim_genero", "sk_genero")
salvar_refined(dim_genero, "dim_genero")

# Criando ponte_filme_genero 
logger.info("Construindo a ponte_filme_genero...")
df_generos_exploded = df_movies_trusted.select("id_filme", explode(split(col("genero"), ",")).alias("nome_genero")).select("id_filme", trim(col("nome_genero")).alias("nome_genero"))
ponte_temp1 = df_generos_exploded.join(dim_filme_final.select("id_filme_movies", "sk_filme"), col("id_filme") == col("id_filme_movies"), how="inner")
ponte_temp2 = ponte_temp1.join(dim_genero, on="nome_genero", how="inner")
ponte_filme_genero = ponte_temp2.select("sk_filme", "sk_genero").distinct() 
ponte_filme_genero = validar_tabela(ponte_filme_genero, "ponte_filme_genero", ["sk_filme", "sk_genero"])
salvar_refined(ponte_filme_genero, "ponte_filme_genero")

#  Criando a fato_participacao de forma mais robusta
logger.info("Construindo a fato_participacao...")

# Coletar participantes de todas as fontes 
participantes_movies = df_movies_trusted.select(
    "id_filme", 
    "nome_artista", 
    "personagem", 
    "genero_artista"
).dropna(subset=["nome_artista"])

# Participantes do TMDB (atores) 
participantes_tmdb = df_atores_trusted.select(
    "id_filme",
    col("ator").alias("nome_artista"),
    lit("Personagem Principal").alias("personagem"),
    lit("Masculino").alias("genero_artista")
)

# Unificar todos os participantes 
todos_participantes = participantes_movies.union(participantes_tmdb).distinct()

# Identificar filmes com protagonistas femininas
df_filmes_protagonistas_fem = df_protagonista_trusted.select(col("id_filme").alias("id_filme_protagonista")).distinct()

# Processar participantes
part_temp2 = todos_participantes.join(df_filmes_protagonistas_fem, todos_participantes.id_filme == df_filmes_protagonistas_fem.id_filme_protagonista, how="left")

part_temp3 = part_temp2.withColumn(
    "is_protagonista",
    when(
        (col("personagem").isNotNull() & (col("personagem") != "")) | 
        (col("id_filme_protagonista").isNotNull() & (col("genero_artista") == 'Feminino')) |
        (lower(col("personagem")).contains("protagonist")) |
        (lower(col("personagem")).contains("main")) |
        (lower(col("personagem")).contains("lead")),
        True
    ).otherwise(False)
)

part_temp4 = part_temp3.join(dim_filme_final.select("id_filme_movies", "sk_filme"), col("id_filme") == col("id_filme_movies"), how="inner")
dim_artista_para_join = dim_artista.select("sk_artista", "nome_artista")
part_temp5 = part_temp4.join(dim_artista_para_join, on="nome_artista", how="inner")

fato_participacao = part_temp5.select(
    "sk_filme",
    "sk_artista",
    "personagem",
    "is_protagonista",
    col("genero_artista").alias("genero_artista_participacao")
).distinct()  

fato_participacao = validar_tabela(fato_participacao, "fato_participacao", ["sk_filme", "sk_artista"])
salvar_refined(fato_participacao, "fato_participacao")

# Construindo a fato_filme de forma mais robusta
logger.info("Construindo fato_filme...")

# Coletar métricas financeiras 
metricas_financeiras = df_atores_trusted.select(
    col("filme_id").alias("id_filme_fin"),
    col("filme_orcamento"),
    col("filme_receita"),
    col("filme_nota_media")
).filter((col("filme_orcamento") > 0) | (col("filme_receita") > 0)).distinct()

# Coletar diretores
diretores_filmes = df_diretores_trusted.select(
    col("filme_id").alias("id_filme_dir"),
    col("diretor_id")
).distinct()

# Coletar datas
datas_filmes = df_atores_trusted.select(
    col("filme_id").alias("id_filme_data"),
    col("filme_data_lancamento")
).union(
    df_diretores_trusted.select(
        col("filme_id").alias("id_filme_data"),
        col("filme_data_lancamento")
    )
).distinct()

# Selecionar colunas necessárias da dim_filme_final
colunas_dim_filme = [
    "sk_filme", 
    "id_filme_tmdb", 
    "id_filme_movies",
    "numero_votos",
    "nota_media",
    "orcamento",
    "receita"
]

fato_filme = dim_filme_final.select(*colunas_dim_filme).alias("dim_f") \
    .join(metricas_financeiras.alias("fin"), 
          col("dim_f.id_filme_tmdb") == col("fin.id_filme_fin"), 
          how="left") \
    .join(diretores_filmes.alias("dir"), 
          col("dim_f.id_filme_tmdb") == col("dir.id_filme_dir"), 
          how="left") \
    .join(datas_filmes.alias("dt"), 
          col("dim_f.id_filme_tmdb") == col("dt.id_filme_data"), 
          how="left") \
    .join(dim_diretor.select("id_diretor", "sk_diretor").alias("dim_d"), 
          col("dir.diretor_id") == col("dim_d.id_diretor"), 
          how="left") \
    .join(dim_data.select("data_completa", "sk_data").alias("dim_dt"), 
          col("dt.filme_data_lancamento") == col("dim_dt.data_completa"), 
          how="left") \
    .select(
    col("dim_f.sk_filme"),
    coalesce(col("dim_d.sk_diretor"), lit(0)).alias("sk_diretor"),
    coalesce(col("dim_dt.sk_data"), lit(0)).alias("sk_data_lancamento"),
    coalesce(
        col("fin.filme_nota_media"), 
        col("dim_f.nota_media"),
        lit(0)
    ).alias("nota_media_final"), 
    coalesce(col("dim_f.numero_votos"), lit(0)).alias("numero_votos"),
    coalesce(col("fin.filme_orcamento"), col("dim_f.orcamento"), lit(0)).alias("orcamento"),
    coalesce(col("fin.filme_receita"), col("dim_f.receita"), lit(0)).alias("receita"),
    (coalesce(col("fin.filme_receita"), col("dim_f.receita"), lit(0)) - coalesce(col("fin.filme_orcamento"), col("dim_f.orcamento"), lit(0))).alias("lucro")
)

# Garantir tipos de dados corretos
fato_filme = fato_filme \
    .withColumn("sk_filme", col("sk_filme").cast("int")) \
    .withColumn("sk_diretor", col("sk_diretor").cast("int")) \
    .withColumn("sk_data_lancamento", col("sk_data_lancamento").cast("int")) \
    .withColumn("nota_media_final", col("nota_media_final").cast("float")) \
    .withColumn("numero_votos", col("numero_votos").cast("int")) \
    .withColumn("orcamento", col("orcamento").cast("bigint")) \
    .withColumn("receita", col("receita").cast("bigint")) \
    .withColumn("lucro", col("lucro").cast("bigint"))

# Window function para pegar o registro mais completo
window_fato = Window.partitionBy("sk_filme").orderBy(
    col("numero_votos").desc(),
    col("nota_media_final").desc(),
    col("receita").desc()
)
fato_filme_final = fato_filme.withColumn("row_num", row_number().over(window_fato)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

fato_filme_final = validar_tabela(fato_filme_final, "fato_filme", "sk_filme")
salvar_refined(fato_filme_final, "fato_filme")

logger.info("Validação das análises...")
# Verificar se temos dados para cada análise
analises = {
    "Protagonistas femininas anos 80": dim_filme_final.filter(col("decada_protagonista_feminina") == "anos_80").count(),
    "Filmes Stephen King": dim_filme_final.filter(col("baseado_em_stephen_king") == True).count(),
    "Filmes terror psicológico": dim_filme_final.filter(col("categoria_terror").isNotNull() & (col("categoria_terror") != "Outros")).count(),
    "Filmes com classificação": dim_filme_final.filter(col("classificacao_indicativa").isNotNull() & (col("classificacao_indicativa") != "Não Classificado")).count(),
    "Filmes com dados financeiros": fato_filme_final.filter(col("orcamento") > 0).count(),
    "Total de filmes na dim_filme": dim_filme_final.count(),
    "Total de artistas na dim_artista": dim_artista.count(),
    "Total de diretores na dim_diretor": dim_diretor.count()
}

for analise, count in analises.items():
    logger.info(f"{analise}: {count}")

logger.info("Amostra fato_filme...")
fato_filme_final.filter(col("orcamento") > 0).show(10)

logger.info("Amostra dim_filme...")  
dim_filme_final.filter(col("baseado_em_stephen_king") == True).show(10)

# Log final de validação
logger.info("Resumindo tabelas geradas...")
tabelas = {
    "dim_filme": dim_filme_final,
    "dim_data": dim_data,
    "dim_artista": dim_artista,
    "dim_diretor": dim_diretor,
    "dim_genero": dim_genero,
    "fato_filme": fato_filme_final,
    "fato_participacao": fato_participacao,
    "ponte_filme_genero": ponte_filme_genero
}

for tabela_nome, df_tabela in tabelas.items():
    try:
        total = df_tabela.count()
        logger.info(f"{tabela_nome}: {total} registros")
    except Exception as e:
        logger.warn(f"{tabela_nome}: Não foi possível verificar - {str(e)}")

job.commit()
logger.info("Job da camada Refined finalizado com sucesso! Todas as tabelas dimensionais foram geradas.")