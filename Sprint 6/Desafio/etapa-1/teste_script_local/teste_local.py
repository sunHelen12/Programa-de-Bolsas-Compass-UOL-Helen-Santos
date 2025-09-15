# Função para o modo local
def run_local():
    import pandas as pd
    import boto3
    import shutil
    import os
    from dotenv import load_dotenv

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

    bucket_name=os.getenv('S3_BUCKET')
    s3_prefix = 'Raw/Local/CSV/Movies/2025/08/27'
    local_download_dir = 'temp_csv_s3'

    # Baixando os arquivos do S3
    print(f"Baixando arquivos do S3: s3://{bucket_name}/{s3_prefix}")
    if not os.path.exists(local_download_dir):
        os.makedirs(local_download_dir)

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
        if 'Contents' not in response:
            print(f"Nenhum arquivo encontrado em '{s3_prefix}'.")
            return

        input_files = []
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.csv'):
                local_file_path = os.path.join(local_download_dir, os.path.basename(key))
                print(f"Baixando '{key}' para '{local_file_path}'...")
                s3_client.download_file(bucket_name, key, local_file_path)
                input_files.append(local_file_path)

    except Exception as e:
        print(f"Erro ao baixar arquivos do S3: {e}")
        return

    if not input_files:
        print("Nenhum arquivo CSV foi baixado. Encerrando.")
        return
        
    # Processo de limpeza com pandas
    print("\nLendo e combinando arquivos CSV baixados...")
    df = pd.concat([pd.read_csv(f, sep='|', na_values=['\\N']) for f in input_files], ignore_index=True)
    print("Leitura concluída. Registros lidos:", len(df))

    # Usando a lógica de limpeza do sript do glue para teste local
    print("Renomeando colunas...")
    column_mapping = {
        'id': 'id_filme',
        'tituloPincipal': 'titulo_principal',
        'tituloOriginal': 'titulo_original',
        'anoLancamento': 'ano_lancamento',
        'tempoMinutos': 'tempo_minutos',
        'genero': 'genero', 
        'notaMedia': 'nota_media',
        'numeroVotos': 'numero_votos',
        'generoArtista': 'genero_artista',
        'nomeArtista': 'nome_artista',
        'anoNascimento': 'ano_nascimento',
        'anoFalecimento': 'ano_falecimento', 
        'profissao': 'profissao',
        'titulosMaisConhecidos': 'titulos_mais_conhecidos',
        'personagem': 'personagem'
    }

    df.rename(columns=column_mapping, inplace=True)
    
    print("Tratando valores ausentes e ajustando tipos...")
    df.dropna(subset=["ano_lancamento"], inplace=True)
    fill_values = {
        "tempo_minutos": 0, "ano_nascimento": 0, "ano_falecimento": 0, "genero": "Desconhecido", 
        "personagem": "N/A", "profissao": "N/A", "titulos_mais_conhecidos": "N/A"
    }
    df.fillna(value=fill_values, inplace=True)
    integer_columns = ["ano_lancamento", "tempo_minutos", "ano_nascimento", "ano_falecimento", "numero_votos"]
    for col_name in integer_columns:
        df[col_name] = df[col_name].astype(int)
    df["nota_media"] = df["nota_media"].astype(float)
        
    print("3. Removendo linhas duplicadas...")
    df.drop_duplicates(inplace=True)
    print("Limpeza concluída. Registros restantes:", len(df))
    
    # Escrita do Arquivo Local
    pasta_resultado = "resultado_local"
    arquivo_saida = "movies_limpo.parquet"

    if not os.path.exists(pasta_resultado):
        os.makedirs(pasta_resultado)

    caminho_saida = os.path.join(pasta_resultado, arquivo_saida)
    print(f"\nGravando arquivo limpo em formato Parquet: {caminho_saida}")
    df.to_parquet(caminho_saida, index=False)
    
    # Limpeza dos arquivos e da pasta temporária 
    print(f"Limpando arquivos CSV temporários da pasta '{local_download_dir}'...")
    try:
        shutil.rmtree(local_download_dir)
        print("Limpeza da pasta temporária concluída com sucesso.")
    except Exception as e:
        print(f"Erro ao limpar a pasta temporária: {e}")

    print(f"\nProcesso local finalizado! O arquivo '{caminho_saida}' foi salvo.")

    # Visualizando uma amostra do arquivo em formato parquet
    df_parquet = pd.read_parquet(caminho_saida)
    print(df_parquet.head())

# Bloco Principal 
if __name__ == "__main__":
    run_local()
    