import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType
from awsglue.dynamicframe import DynamicFrame

# Leitura de parâmetros 
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "S3_INPUT_PATH",
    "S3_OUTPUT_PATH_BASE", 
    "GLUE_DB_NAME"
])

# Inicialização do Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# Atribuindo as variáveis a partir dos parâmetros
S3_INPUT_PATH = args['S3_INPUT_PATH']
S3_OUTPUT_PATH_BASE = args['S3_OUTPUT_PATH_BASE']
GLUE_DB_NAME = args['GLUE_DB_NAME']

# Leitura dos Dados da Raw Zone
logger.info(f"Iniciando leitura dos dados CSV de: {S3_INPUT_PATH}")
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [S3_INPUT_PATH], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "separator": "|", "quoteChar": '"'},
)
df = dynamic_frame.toDF()
logger.info(f"Leitura concluída. Total de registros lidos: {df.count()}")

# Processo de Limpeza

# Renomeando Colunas
logger.info("Iniciando renomeação de colunas para o padrão...")
column_mapping = {
    'id': 'id_filme', 'tituloPincipal': 'titulo_principal', 'tipoTitulo': 'tipo_titulo',
    'anoLancamento': 'ano_lancamento', 'tempoMinutos': 'tempo_minutos', 'genero': 'genero',
    'notaMedia': 'nota_media', 'numeroVotos': 'numero_votos', 'idArtista': 'id_artista',
    'nomeArtista': 'nome_artista', 'anoNascimento': 'ano_nascimento', 'anoFalecimento': 'ano_falecimento',
    'profissao': 'profissao', 'titulosMaisConhecidos': 'titulos_mais_conhecidos', 'personagem': 'personagem'
}
for old_name, new_name in column_mapping.items():
    df = df.withColumnRenamed(old_name, new_name)

# Tratar Valores Ausentes e Ajustar Tipos
logger.info("Iniciando tratamento de valores ausentes e ajuste de tipos de dados.")
df = df.dropna(subset=["ano_lancamento"])
df = df.fillna(0, subset=["tempo_minutos", "ano_nascimento", "ano_falecimento"])
df = df.fillna("Desconhecido", subset=["genero"])
df = df.fillna("N/A", subset=["personagem", "profissao", "titulos_mais_conhecidos"])

integer_columns = ["ano_lancamento", "tempo_minutos", "ano_nascimento", "ano_falecimento", "numero_votos"]
for col_name in integer_columns:
    df = df.withColumn(col_name, col(col_name).cast(IntegerType()))

df = df.withColumn("nota_media", col("nota_media").cast(FloatType()))

# Removendo Duplicatas
logger.info("Iniciando remoção de linhas duplicadas.")
df = df.dropDuplicates()
logger.info(f"Limpeza concluída. Total de registros restantes: {df.count()}")

# Escrita na Trusted Zone
logger.info(f"Gravando dados limpos em formato Parquet em: {S3_OUTPUT_PATH_BASE}")
dynamic_frame_to_write = DynamicFrame.fromDF(df, glueContext, "cleaned_df")

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_to_write,
    connection_type="s3",
    connection_options={
        "path": S3_OUTPUT_PATH_BASE, 
        "database": GLUE_DB_NAME
    },
    format="parquet"
)

job.commit()
