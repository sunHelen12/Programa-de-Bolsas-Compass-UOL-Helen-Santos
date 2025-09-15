# Script para execução local com PySpark
def run_local():
    import os
    import boto3
    import shutil
    from dotenv import load_dotenv
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, explode, input_file_name, lit

    # Inicialização do Spark Localmente 
    print("Iniciando a sessão Spark local...")
    spark = SparkSession.builder \
        .appName("Teste Local Job Glue com Boto3") \
        .master("local[*]") \
        .getOrCreate()

    # Download dos arquivos JSON do S3 com Boto3
    print("Iniciando download de arquivos do S3...")

    # Garantindo que as variáveis do .env sejam carregadas
    load_dotenv()

    # Configurando S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.getenv('AWS_SESSION_TOKEN'), 
        region_name=os.getenv('AWS_REGION'),
    )

    # Configurando variáveis
    nome_bucket =os.getenv('S3_BUCKET') 
    S3_PREFIXO = f"Raw/TMDB/JSON/{'2025'}/{'09'}/{'01'}/"

    # Caminhos para entrada e saída de arquivos
    dados_locais = "dados_entrada_json/" # Pasra temporário
    pasta_saida = "dados_saida_trusted/"

    # Garante que as pastas locais existam
    if not os.path.exists(dados_locais):
        os.makedirs(dados_locais)
    if not os.path.exists(pasta_saida):
        os.makedirs(pasta_saida)

    try:
        # Lista os objetos no bucket 
        response = s3_client.list_objects_v2(Bucket=nome_bucket, Prefix=S3_PREFIXO)

        if 'Contents' not in response or len(response['Contents']) == 0:
            print(f"AVISO: Nenhum arquivo encontrado em s3://{nome_bucket}/{S3_PREFIXO}")
            spark.stop()
            exit()
        else:
            for obj in response['Contents']:
                s3_key = obj['Key']
                nome_arquivo = os.path.basename(s3_key)

                # Ignora se for a própria pasta retornada pela listagem
                if not nome_arquivo:
                    continue

                pasta_local = os.path.join(dados_locais, nome_arquivo)
                print(f"Baixando '{s3_key}' para '{pasta_local}'...")
                s3_client.download_file(nome_bucket, s3_key, pasta_local)
        
        print("Download do S3 concluído com sucesso.")

    except Exception as e:
        print(f"\nERRO ao conectar ou baixar arquivos do S3: {e}")
        spark.stop()
        exit() 

    # Salvando arquivo localmente
    def salvar_localmente(df, nome_tabela):
        caminho_final = os.path.join(pasta_saida, nome_tabela)
        print(f"Gravando tabela '{nome_tabela}' em: {caminho_final}")
        df.write.mode("overwrite").parquet(caminho_final)
        print(f"Dados de '{nome_tabela}' salvos com sucesso.")

    # Leitura dos Arquivos JSON 
    print(f"\nLendo todos os arquivos JSON da pasta local '{dados_locais}'...")
    df_raw = spark.read.option("multiline", "true").json(dados_locais)
    df_raw = df_raw.withColumn("source_file", input_file_name())

    # Tratamento de Atores e Diretores
    print("Processando arquivos de atores e diretores...")

    # Atores
    df_atores = df_raw.filter(col("source_file").contains("atores_terror_moderno"))
    if not df_atores.rdd.isEmpty():
        df_atores_exploded = df_atores.select(explode(col("filmografia_terror")).alias("filme"), col("ator"), col("id").alias("ator_id"))
        df_atores_final = df_atores_exploded.select(
            col("ator"), col("ator_id"), col("filme.id").alias("filme_id"), col("filme.title").alias("filme_titulo"),
            col("filme.release_date").alias("filme_data_lancamento"), col("filme.vote_average").alias("filme_nota_media"),
            col("filme.budget").alias("filme_orcamento"), col("filme.revenue").alias("filme_receita")
        )
        df_atores_final = df_atores_final.filter(col("filme_id").isNotNull())
        print(f"Total de filmes de atores após limpeza: {df_atores_final.count()}")
        salvar_localmente(df_atores_final, "atores_filmografia_trusted")

    # Diretores
    df_diretores = df_raw.filter(col("source_file").contains("mestres_suspense"))
    if not df_diretores.rdd.isEmpty():
        df_diretores_exploded = df_diretores.select(explode(col("filmografia_suspense_misterio")).alias("filme"), col("diretor"), col("id").alias("diretor_id"))
        df_diretores_final = df_diretores_exploded.select(
            col("diretor"), col("diretor_id"), col("filme.id").alias("filme_id"), col("filme.title").alias("filme_titulo"),
            col("filme.release_date").alias("filme_data_lancamento"), col("filme.vote_average").alias("filme_nota_media")
        )
        df_diretores_final = df_diretores_final.filter(col("filme_id").isNotNull())
        print(f"Total de filmes de diretores após limpeza: {df_diretores_final.count()}")
        salvar_localmente(df_diretores_final, "diretores_filmografia_trusted")
        
    # Tratamento de arquivos de comparação
    def processando_comparacao(df_completa, nome_arquivo, colunas, coluna_nova):
        print(f"Processando arquivo {nome_arquivo}...")
        df_filtrado = df_completa.filter(col("source_file").contains(nome_arquivo))
        if df_filtrado.rdd.isEmpty():
            print(f"Aviso: Nenhum dado encontrado para {nome_arquivo}")
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
                col(coluna_nova), col("filme.id").alias("filme_id"), col("filme.title").alias("filme_titulo"),
                col("filme.release_date").alias("filme_data_lancamento"), col("filme.vote_average").alias("filme_nota_media")
            )
            df_final = df_final.filter(col("filme_id").isNotNull())
            print(f"Total de registros para '{nome_arquivo}' após limpeza: {df_final.count()}")
            salvar_localmente(df_final, f"{nome_arquivo}_trusted")

    # Processando cada arquivo de comparação
    processando_comparacao(df_raw, "protagonista_feminina", ["anos_80", "anos_2020"], "decada")
    processando_comparacao(df_raw, "terror_classificacao", ["terror_pg13", "terror_r"], "classificacao")
    processando_comparacao(df_raw, "stephen_king", ["stephen_king", "originais"], "fonte")
    processando_comparacao(df_raw, "terror_psicologico_misterio", ["terror_psicologico", "terror_psicologico_misterio"], "categoria")

    print("\nProcessamento local concluído!")

    # Limpeza de pasta temporária
    try:
        if os.path.exists(dados_locais):
            print(f"Limpando a pasta temporária: {dados_locais}")
            shutil.rmtree(dados_locais)
            print("Limpeza concluída com sucesso.")
    except Exception as e:
        print(f"ERRO ao limpar a pasta temporária: {e}")

    spark.stop()
    
# Bloco Principal 
if __name__ == "__main__":
    run_local()
    
