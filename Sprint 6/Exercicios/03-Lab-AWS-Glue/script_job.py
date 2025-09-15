import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import upper, desc, count, sum as spark_sum, asc
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH','S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()

source_file = args['S3_INPUT_PATH']                                                                         
target_path = args['S3_TARGET_PATH']

logger.info(f"Iniciando a leitura do arquivo em: {source_file}")
dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [source_file]
    },
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ","
    }
)

df = dyf.toDF()
df = df.withColumn("total", df["total"].cast("integer"))

logger.info("Schema do DataFrame")
df.printSchema()

df_nomes_maiusculo = df.withColumn("nome", upper(df["nome"]))
logger.info("DataFrame com nomes em maísuclo(amostra)")
df_nomes_maiusculo.show(5)

total_linhas = df_nomes_maiusculo.count()
logger.info(f"Total de linhas no DataFrame: {total_linhas}")

contagem_por_ano_sexo = df_nomes_maiusculo.groupBy("ano", "sexo").agg(count("nome").alias("total_nomes"))
logger.info("Contagem de nomes por ano e sexo (amostra):")
contagem_por_ano_sexo.show(10)

df_ordenado = df_nomes_maiusculo.orderBy(desc("ano"))
logger.info("DataFrame df_ordenado pelo ano mais recente (amostra")
df_ordenado.show(5)

nome_feminino_mais_registros = df_nomes_maiusculo.filter(df["sexo"] == "F").orderBy(desc("total")).first()
logger.info(f"Nome feminino com mais registros: {nome_feminino_mais_registros['nome']}, Ano:{nome_feminino_mais_registros['ano']}, Total: {nome_feminino_mais_registros['total']}")

nome_masculino_mais_registros = df_nomes_maiusculo.filter(df["sexo"] == "M").orderBy(desc("total")).first()
logger.info(f"Nome masculino com mais registros: {nome_masculino_mais_registros['nome']}, Ano: {nome_masculino_mais_registros['ano']}, Total: {nome_masculino_mais_registros['total']}")

total_registros_por_ano = df_nomes_maiusculo.groupBy("ano").agg(spark_sum("total").alias("total_registros")).orderBy(asc("ano"))
logger.info("Total de registros por ano (10 primeiros anos):")
total_registros_por_ano.show(10)


logger.info(f"Iniciando a gravação dos dados em {target_path}")
dy_final = DynamicFrame.fromDF(df_nomes_maiusculo, glueContext, "dy_final")

glueContext.write_dynamic_frame.from_options(
    frame = dy_final,
    connection_type = "s3",
    connection_options = {
        "path": target_path,
        "partitionKeys": [
            "sexo",
            "ano"
        ]
    },
    format = "json"
)

logger.info("Dados gravados com sucesso no S3.")

job.commit()