# Script para execução do Job no AWS Glue
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, input_file_name, lit
from awsglue.dynamicframe import DynamicFrame

# Leitura dos parâmetros 
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "GLUE_DB_NAME",
    "S3_INPUT_PATH_TMDB",
    "S3_TRUSTED_PATH_BASE"
])

# Inicializa os componentes essenciais para o Job no Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

#Configuração de Variáveis
S3_INPUT_PATH = args['S3_INPUT_PATH_TMDB']
S3_OUTPUT_PATH = args['S3_TRUSTED_PATH_BASE']
GLUE_DB_NAME = args['GLUE_DB_NAME']

# Gerando caminho de saída
hoje = datetime.now()
S3_OUTPUT_PATH_FINAL = f"{S3_OUTPUT_PATH}{hoje.year}/{hoje.month:02d}/{hoje.day:02d}/" 

# Função para gravar os dados
def salvano_trusted_zone(df, nome_tabela):
    # Escrevendo os dados no S3  
    logger.info(f"Gravando tabela '{nome_tabela}' em: {S3_OUTPUT_PATH_FINAL}{nome_tabela}/")
    dynamic_frame_to_write = DynamicFrame.fromDF(df, glueContext, f"df_{nome_tabela}")   
    
    # Criação de uma pasta para cada tabela ---
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame_to_write,
        connection_type="s3",        
        connection_options={
            "path": f"{S3_OUTPUT_PATH_FINAL}{nome_tabela}/", 
            "database": GLUE_DB_NAME
        },
        format="parquet"
    )
    logger.info(f"Dados {nome_tabela} salvos com sucesso em S3.")

# Tratamento de Atores e Diretores
logger.info("Processando arquivos de atores e diretores...")

# Atores
try:
    # Lendo o arquivo específico dos atores 
    logger.info("Lendo arquivo de atores...")
    df_atores_raw = spark.read.option("multiline", "true").json(f"{S3_INPUT_PATH}atores_terror_moderno.json")
    
    if not df_atores_raw.rdd.isEmpty():
        df_atores_exploded = df_atores_raw.select(explode(col("filmografia_terror")).alias("filme"), col("ator"), col("id").alias("ator_id"))
        df_atores_final = df_atores_exploded.select(
            col("ator"), col("ator_id"), col("filme.id").alias("filme_id"), col("filme.title").alias("filme_titulo"),
            col("filme.release_date").alias("filme_data_lancamento"), col("filme.vote_average").alias("filme_nota_media"),
            col("filme.budget").alias("filme_orcamento"), col("filme.revenue").alias("filme_receita")
        )
        df_atores_final = df_atores_final.filter(col("filme_id").isNotNull())
        logger.info(f"Total de filmes de atores após limpeza: {df_atores_final.count()}")
        salvano_trusted_zone(df_atores_final, "atores_filmografia_trusted")
except Exception as e:
    logger.error(f"Erro ao processar arquivo de atores: {e}")


# Diretores
try:
    # Lendo o arquivo específico dos diretores 
    logger.info("Lendo arquivo de diretores...")
    df_diretores_raw = spark.read.option("multiline", "true").json(f"{S3_INPUT_PATH}mestres_suspense.json")

    if not df_diretores_raw.rdd.isEmpty():
        df_diretores_exploded = df_diretores_raw.select(explode(col("filmografia_suspense_misterio")).alias("filme"), col("diretor"), col("id").alias("diretor_id"))
        df_diretores_final = df_diretores_exploded.select(
            col("diretor"), col("diretor_id"), col("filme.id").alias("filme_id"), col("filme.title").alias("filme_titulo"),
            col("filme.release_date").alias("filme_data_lancamento"), col("filme.vote_average").alias("filme_nota_media")
        )
        df_diretores_final = df_diretores_final.filter(col("filme_id").isNotNull())
        logger.info(f"Total de filmes de diretores após limpeza: {df_diretores_final.count()}")
        salvano_trusted_zone(df_diretores_final, "diretores_filmografia_trusted")
except Exception as e:
    logger.error(f"Erro ao processar arquivo de diretores: {e}")


# Tratamento de arquivos de comparação
def processando_comparacao(nome_arquivo, colunas, coluna_nova):
    try:
        logger.info(f"Processando arquivo {nome_arquivo}...")
        df_filtrado = spark.read.option("multiline", "true").json(f"{S3_INPUT_PATH}{nome_arquivo}.json")
        
        if df_filtrado.rdd.isEmpty():
            logger.warning(f"Nenhum dado encontrado para {nome_arquivo}")
            return
        
        df_uniao = None
        for col_name in colunas:
            if col_name in df_filtrado.columns:
                df_temporaria = df_filtrado.select(explode(col(f"{col_name}.filmes")).alias("filme")).withColumn(coluna_nova, lit(col_name))
                if df_uniao is None:
                    df_uniao = df_temporaria
                else:
                    df_uniao = df_uniao.unionByName(df_temporaria, allowMissingColumns=True)

        if df_uniao:
            df_final = df_uniao.select(
                col(coluna_nova), 
                col("filme.id").alias("filme_id"), 
                col("filme.title").alias("filme_titulo"), 
                col("filme.release_date").alias("filme_data_lancamento"), 
                col("filme.vote_average").alias("filme_nota_media")
            )
            df_final = df_final.filter(col("filme_id").isNotNull())
            logger.info(f"Total de registros para '{nome_arquivo}' após limpeza: {df_final.count()}")
            salvano_trusted_zone(df_final, f"{nome_arquivo}_trusted")
    except Exception as e:
        logger.error(f"Erro ao processar arquivo de comparação '{nome_arquivo}': {e}")


# Processando cada arquivo de comparação
processando_comparacao("protagonista_feminina", ["anos_80", "anos_2020"], "decada")
processando_comparacao("terror_classificacao", ["terror_pg13", "terror_r"], "classificacao")
processando_comparacao("stephen_king", ["stephen_king", "originais"], "fonte")
processando_comparacao("terror_psicologico_misterio", ["terror_psicologico", "terror_psicologico_misterio"], "categoria")

job.commit()






