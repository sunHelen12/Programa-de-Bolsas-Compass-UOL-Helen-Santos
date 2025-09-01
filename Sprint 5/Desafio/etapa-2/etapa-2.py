# Importando bibliotecas
import json
import os
from datetime import datetime
from tmdbv3api import TMDb, Movie, Person, Discover
from dotenv import load_dotenv
import boto3
import time

# Carregar variáveis do .env
load_dotenv()

# Configuração do TMDB
tmdb = TMDb()
movie = Movie()
person = Person()
tmdb.api_key = os.getenv("TMDB_API_KEY")

# Configuração do S3 
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("AWS_REGION")
)
BUCKET_S3 = os.getenv("S3_BUCKET")

# Modo de operação (desenvolvimento ou produção)
MODO_DESENVOLVIMENTO = os.getenv("MODO_DESENVOLVIMENTO", "True").lower() == "true"

# Pasta local para salvar os JSONs - Com Condicional
PASTA_RESULTADOS = "resultados_json"
if MODO_DESENVOLVIMENTO:
    if not os.path.exists(PASTA_RESULTADOS):
        os.makedirs(PASTA_RESULTADOS)

# Função simplificada para converter objetos para dicionário
def objeto_para_dict(obj):
    if hasattr(obj, '__dict__'):
        return {k: objeto_para_dict(v) for k, v in obj.__dict__.items() if not k.startswith('_')}
    elif isinstance(obj, list):
        return [objeto_para_dict(item) for item in obj]
    else:
        return obj

# Função para agrupar registros
def agrupar_registros(dados, max_registros=100):
    if not isinstance(dados, list):
        return [dados]
    
    grupos = []
    for i in range(0, len(dados), max_registros):
        grupos.append(dados[i:i+max_registros])
    return grupos

# Função para salvar JSON localmente
def salvar_json_local(nome_arquivo, dados):
    caminho = os.path.join(PASTA_RESULTADOS, f"{nome_arquivo}.json")
    with open(caminho, "w", encoding="utf-8") as arquivo:
        json.dump(dados, arquivo, ensure_ascii=False, indent=2)
    print(f"Arquivo salvo localmente: {caminho}")
    return caminho

# Função para salvar JSON no S3 
def salvar_s3(nome_arquivo, dados):
    if MODO_DESENVOLVIMENTO:
        print(f"Modo desenvolvimento: Simulando envio para S3 - {nome_arquivo}")
        return True
    
    data_atual = datetime.now()
    path_s3 = f"Raw/TMDB/JSON/{data_atual.year}/{data_atual.month:02}/{data_atual.day:02}/{nome_arquivo}.json"
    
    try:
        s3.put_object(
            Bucket=BUCKET_S3, 
            Key=path_s3, 
            Body=json.dumps(dados, ensure_ascii=False, indent=2)
        )
        print(f"Arquivo enviado para o S3: {path_s3}")
        return True
    except Exception as e:
        print(f"Erro ao enviar para S3: {e}")
        return False

# Função para processar e salvar dados localmente (dev) ou no S3 (prod)
def processar_e_salvar_dados(nome_base, dados):   
    if not dados:
        print(f"Nenhum dado encontrado para {nome_base}")
        return
    
    grupos = agrupar_registros(dados, 100)
    
    for i, grupo in enumerate(grupos):
        nome_arquivo = f"{nome_base}_parte_{i+1}" if len(grupos) > 1 else nome_base
        
        # Só salva localmente se estiver em modo de desenvolvimento
        if MODO_DESENVOLVIMENTO:
            salvar_json_local(nome_arquivo, grupo)
        
        # Sempre envia para o S3 (ou simula, se em modo de desenvolvimento)
        salvar_s3(nome_arquivo, grupo)

def buscar_multiplas_paginas(discover, params, max_paginas=3):
    """Busca múltiplas páginas de resultados"""
    todos_filmes = []
    
    for pagina in range(1, max_paginas + 1):
        try:
            print(f"Buscando página {pagina}...")
            params["page"] = pagina
            resultados = discover.discover_movies(params)
            
            if hasattr(resultados, 'results'):
                todos_filmes.extend(resultados.results)
            
            # Delay para não sobrecarregar a API
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Erro na página {pagina}: {e}")
            break
    
    return todos_filmes

# Função para verificar protagonista feminina
def verificar_protagonista_feminina(filme_id):
    try:
        detalhes = movie.details(filme_id)
        creditos = movie.credits(filme_id)
        
        if (hasattr(creditos, 'cast') and len(creditos.cast) > 0):
            protagonista = creditos.cast[0]
            # Gênero 1 = feminino, 2 = masculino
            if hasattr(protagonista, 'gender') and protagonista.gender == 1:
                return True, protagonista.name
        
        return False, None
        
    except Exception as e:
        print(f"Erro ao verificar protagonista do filme {filme_id}: {e}")
        return False, None

# 1. Análise: Evolução da Protagonista Feminina 
def analise_protagonista_feminina():
    discover = Discover()
    resultado_total = {}

    try:
        # Buscar filmes dos anos 80 (3 páginas)
        print("Buscando filmes de terror anos 80...")
        filmes_anos_80 = buscar_multiplas_paginas(discover, {
            "with_genres": 27,
            "primary_release_date.gte": "1980-01-01",
            "primary_release_date.lte": "1989-12-31"
        }, max_paginas=3)

        # Buscar filmes dos anos 2020 (3 páginas)
        print("Buscando filmes de terror anos 2020...")
        filmes_anos_2020 = buscar_multiplas_paginas(discover, {
            "with_genres": 27,
            "primary_release_date.gte": "2020-01-01",
            "primary_release_date.lte": "2024-12-31"
        }, max_paginas=3)

        # Filtrar apenas filmes com protagonistas femininas - Anos 80
        filmes_80_femininos = []
        for filme in filmes_anos_80:
            try:
                tem_protagonista_feminina, nome_ator = verificar_protagonista_feminina(filme.id)
                
                if tem_protagonista_feminina:
                    filme_data = {
                        "id": filme.id,
                        "title": filme.title,
                        "release_date": filme.release_date,
                        "vote_average": filme.vote_average,
                        "vote_count": filme.vote_count,
                        "protagonista_feminina": True,
                        "nome_protagonista": nome_ator
                    }
                    filmes_80_femininos.append(filme_data)
                    
            except Exception as e:
                print(f"Erro ao processar filme {filme.id}: {e}")
                continue

        # Filtrar apenas filmes com protagonistas femininas - Anos 2020
        filmes_2020_femininos = []
        for filme in filmes_anos_2020:
            try:
                tem_protagonista_feminina, nome_ator = verificar_protagonista_feminina(filme.id)
                
                if tem_protagonista_feminina:
                    filme_data = {
                        "id": filme.id,
                        "title": filme.title,
                        "release_date": filme.release_date,
                        "vote_average": filme.vote_average,
                        "vote_count": filme.vote_count,
                        "protagonista_feminina": True,
                        "nome_protagonista": nome_ator
                    }
                    filmes_2020_femininos.append(filme_data)
                    
            except Exception as e:
                print(f"Erro ao processar filme {filme.id}: {e}")
                continue

        # Calcular estatísticas apenas para filmes femininos
        def calcular_estatisticas_femininas(filmes_femininos):
            if filmes_femininos:
                nota_media = sum(f["vote_average"] for f in filmes_femininos if f["vote_average"]) / len(filmes_femininos)
                total_votos = sum(f["vote_count"] for f in filmes_femininos)
            else:
                nota_media = 0
                total_votos = 0
                
            return {
                "quantidade_filmes": len(filmes_femininos),
                "nota_media": round(nota_media, 3),
                "total_votos": total_votos
            }

        stats_80 = calcular_estatisticas_femininas(filmes_80_femininos)
        stats_2020 = calcular_estatisticas_femininas(filmes_2020_femininos)

        resultado_total = {
            "anos_80": {
                **stats_80,
                "filmes": filmes_80_femininos  
            },
            "anos_2020": {
                **stats_2020,
                "filmes": filmes_2020_femininos  
            },
            "comparacao": {
                "diferenca_quantidade": stats_2020["quantidade_filmes"] - stats_80["quantidade_filmes"],
                "diferenca_nota_media": round(stats_2020["nota_media"] - stats_80["nota_media"], 3),
                "aumento_percentual_quantidade": round((stats_2020["quantidade_filmes"] - stats_80["quantidade_filmes"]) / stats_80["quantidade_filmes"] * 100, 2) if stats_80["quantidade_filmes"] > 0 else 0
            }
        }

    except Exception as e:
        print(f"Erro na análise: {e}")
        resultado_total = {"erro": str(e)}

    return resultado_total

# Função para buscar filmografia de terror de um ator
def buscar_filmografia_terror(ator_id, nome_ator):
    filmografia_terror = []
    
    try:
        print(f"Buscando filmografia de {nome_ator}...")
        creditos = person.movie_credits(ator_id)        
       
        if hasattr(creditos, 'cast'):
            for filme in creditos.cast:
                # Acessa os genre_ids diretamente do objeto 'filme'
                if hasattr(filme, 'genre_ids') and 27 in filme.genre_ids:
                    try:
                        # O resto do seu código para buscar detalhes continua igual
                        detalhes = movie.details(filme.id)
                        
                        filme_completo = {
                            "id": filme.id,
                            "title": filme.title,
                            "release_date": filme.release_date if hasattr(filme, 'release_date') else None,
                            "vote_average": filme.vote_average,
                            "vote_count": filme.vote_count,
                            "character": filme.character if hasattr(filme, 'character') else None,
                            "overview": detalhes.overview,
                            "revenue": detalhes.revenue,
                            "budget": detalhes.budget,
                            "runtime": detalhes.runtime,
                            "poster_path": detalhes.poster_path
                        }
                        
                        # Calcular ROI se possível
                        if filme_completo['budget'] > 0 and filme_completo['revenue'] > 0:
                            filme_completo['roi'] = round(
                                (filme_completo['revenue'] - filme_completo['budget']) / 
                                filme_completo['budget'] * 100, 2
                            )
                        else:
                            filme_completo['roi'] = None
                            
                        filmografia_terror.append(filme_completo)
                        
                    except Exception as e:
                        print(f"Erro ao buscar detalhes do filme {filme.id}: {e}")
                    
                    time.sleep(0.1)  # Delay para não sobrecarregar a API
            
            print(f"Encontrados {len(filmografia_terror)} filmes de terror para {nome_ator}")
        
    except Exception as e:
        print(f"Erro ao buscar filmografia de {nome_ator}: {e}")
    
    return filmografia_terror

# 2. Análise: Reinado dos Ícones Masculinos do Terror Moderno
def analise_atores_terror_moderno():
    atores = [
        {"nome": "Patrick Wilson", "id": 17178},
        {"nome": "Ethan Hawke", "id": 569},
        {"nome": "Tobin Bell", "id": 2144},
        {"nome": "Daniel Kaluuya", "id": 206919}
    ]
    
    resultado_total = []
    
    for ator in atores:
        try:
            # Buscar filmografia de terror
            filmografia = buscar_filmografia_terror(ator["id"], ator["nome"])
            
            # Calcular estatísticas
            if filmografia:
                # Ordenar por data de lançamento
                filmografia_ordenada = sorted(
                    [f for f in filmografia if f.get('release_date')],
                    key=lambda x: x.get('release_date', ''),
                    reverse=True
                )
                
                # Calcular métricas
                notas_validas = [f["vote_average"] for f in filmografia if f.get("vote_average")]
                nota_media = sum(notas_validas) / len(notas_validas) if notas_validas else 0
                
                total_bilheteria = sum(f.get("revenue", 0) for f in filmografia)
                total_orcamento = sum(f.get("budget", 0) for f in filmografia)
                
                # Encontrar filme mais popular
                filmes_com_votos = [f for f in filmografia if f.get('vote_count', 0) > 0]
                if filmes_com_votos:
                    filme_mais_popular = max(filmes_com_votos, key=lambda x: x.get('vote_count', 0))
                else:
                    filme_mais_popular = {}
                
                estatisticas = {
                    "quantidade_filmes_terror": len(filmografia),
                    "nota_media": round(nota_media, 2),
                    "total_bilheteria": total_bilheteria,
                    "total_orcamento": total_orcamento,
                    "lucro_total": total_bilheteria - total_orcamento,
                    "filme_mais_popular": {
                        "titulo": filme_mais_popular.get('title', 'N/A'),
                        "nota": filme_mais_popular.get('vote_average', 0),
                        "votos": filme_mais_popular.get('vote_count', 0)
                    }
                }
            else:
                estatisticas = {}
                filmografia_ordenada = []
            
            resultado_total.append({
                "ator": ator["nome"],
                "id": ator["id"],
                "estatisticas": estatisticas,
                "filmografia_terror": filmografia_ordenada,
                "quantidade_filmes": len(filmografia_ordenada)
            })
            
        except Exception as e:
            print(f"Erro ao processar {ator['nome']}: {e}")
            resultado_total.append({
                "ator": ator["nome"],
                "erro": str(e),
                "filmografia_terror": []
            })
    
    return resultado_total


# 3. Análise: Terror Psicológico e Mistério
def analise_terror_psicologico_misterio():
    discover = Discover()
    resultado_total = {}

    try:
        print("Buscando terror psicológico...")
        terror_psicologico = discover.discover_movies({
            "with_genres": 27,  # Terror
            "with_keywords": "295907",  # ID para Psychological
            "page": 1
        })

        print("Buscando terror psicológico com mistério...")
        terror_misterio = discover.discover_movies({
            "with_genres": "27,9648",  # Terror + Mistério
            "with_keywords": "295907",  # Psychological
            "page": 1
        })

        # Processar resultados
        def processar_resultados(resultados):
            filmes = []
            if hasattr(resultados, 'results'):
                for filme in resultados.results:
                    filmes.append({
                        "id": getattr(filme, 'id', None),
                        "title": getattr(filme, 'title', None),
                        "release_date": getattr(filme, 'release_date', None),
                        "vote_average": getattr(filme, 'vote_average', None)
                    })
            return filmes

        psicologico = processar_resultados(terror_psicologico)
        misterio = processar_resultados(terror_misterio)

        resultado_total = {
            "terror_psicologico": {
                "quantidade": len(psicologico),
                "nota_media": sum(f["vote_average"] for f in psicologico if f["vote_average"]) / len(psicologico) if psicologico else 0,
                "filmes": psicologico
            },
            "terror_psicologico_misterio": {
                "quantidade": len(misterio),
                "nota_media": sum(f["vote_average"] for f in misterio if f["vote_average"]) / len(misterio) if misterio else 0,
                "filmes": misterio
            }
        }

    except Exception as e:
        print(f"Erro na análise de terror psicológico: {e}")
        resultado_total = {"erro": str(e)}

    return resultado_total

# 4. Análise: O Dilema do Terror Leve
def analise_terror_classificacao():
    discover = Discover()
    resultado_total = {}

    try:
        print("Buscando terror PG-13...")
        terror_pg13 = discover.discover_movies({
            "with_genres": 27,
            "certification_country": "US",
            "certification": "PG-13",
            "page": 1
        })

        print("Buscando terror R...")
        terror_r = discover.discover_movies({
            "with_genres": 27,
            "certification_country": "US",
            "certification": "R",
            "page": 1
        })

        # Processar resultados
        def processar_resultados(resultados):
            filmes = []
            if hasattr(resultados, 'results'):
                for filme in resultados.results:
                    filmes.append({
                        "id": getattr(filme, 'id', None),
                        "title": getattr(filme, 'title', None),
                        "release_date": getattr(filme, 'release_date', None),
                        "vote_average": getattr(filme, 'vote_average', None),
                        "popularidade": getattr(filme, 'popularity', None)
                    })
            return filmes

        pg13 = processar_resultados(terror_pg13)
        r = processar_resultados(terror_r)

        resultado_total = {
            "terror_pg13": {
                "quantidade": len(pg13),
                "nota_media": sum(f["vote_average"] for f in pg13 if f["vote_average"]) / len(pg13) if pg13 else 0,
                "filmes": pg13
            },
            "terror_r": {
                "quantidade": len(r),
                "nota_media": sum(f["vote_average"] for f in r if f["vote_average"]) / len(r) if r else 0,
                "filmes": r
            }
        }

    except Exception as e:
        print(f"Erro na análise de classificação: {e}")
        resultado_total = {"erro": str(e)}

    return resultado_total

# 5. Análise: Mestres do Suspense Moderno
def analise_mestres_suspense():
    diretores = ["David Fincher", "Christopher Nolan"]
    resultado_total = []
    pessoa = Person()

    # IDs dos gêneros 
    GENERO_SUSPENSE = 53  # Thriller
    GENERO_MISTERIO = 9648 # Mystery

    for nome in diretores:
        try:
            print(f"Buscando {nome}...")
            pesquisa = pessoa.search(nome)
            
            if not pesquisa:
                resultado_total.append({"diretor": nome, "erro": "Diretor não encontrado"})
                continue

            diretor_info = pesquisa[0]
            diretor_id = getattr(diretor_info, 'id', None)

            filmografia_filtrada = []
            if diretor_id:
                creditos = pessoa.movie_credits(diretor_id)
                
                if hasattr(creditos, 'crew'):
                    for trabalho in creditos.crew:
                        if hasattr(trabalho, 'job') and trabalho.job == 'Director':
                            genre_ids = getattr(trabalho, 'genre_ids', [])
                            
                            if GENERO_SUSPENSE in genre_ids or GENERO_MISTERIO in genre_ids:
                                filmografia_filtrada.append({
                                    "id": getattr(trabalho, 'id', None),
                                    "title": getattr(trabalho, 'title', None),
                                    "release_date": getattr(trabalho, 'release_date', None),
                                    "vote_average": getattr(trabalho, 'vote_average', None),
                                    "vote_count": getattr(trabalho, 'vote_count', None),
                                    "overview": getattr(trabalho, 'overview', None),
                                    # Convertendo a lista de gêneros para uma lista padrão
                                    "genre_ids": list(genre_ids) 
                                })

            resultado_total.append({
                "diretor": nome,
                "id": diretor_id,
                "nome": getattr(diretor_info, 'name', None),
                # Convertendo a popularidade para float
                "popularidade": float(getattr(diretor_info, 'popularity', 0.0)),
                "filmografia_suspense_misterio": sorted(
                    [f for f in filmografia_filtrada if f.get('release_date')],
                    key=lambda x: x.get('release_date', ''),
                    reverse=True
                )
            })
                
        except Exception as e:
            print(f"Erro ao buscar {nome}: {e}")
            resultado_total.append({"diretor": nome, "erro": str(e)})

    return resultado_total

# 6. Análise: O Toque de Midas de Stephen King
def analise_stephen_king():
    discover = Discover()
    resultado_total = {}
    try:
        print("Buscando filmes de Stephen King...")
        # Usando o ID de pessoa 
        king_filmes = discover.discover_movies({"with_crew": "3027", "page": 1})
        
        print("Buscando filmes de terror originais...")
        outros_filmes = discover.discover_movies({"with_genres": 27, "without_keywords": "8231,3027", "page": 1})

        def processar_resultados(resultados):
            filmes = []
            if hasattr(resultados, 'results'):
                for filme in resultados.results:
                    filmes.append({"id": filme.id, "title": filme.title, "release_date": filme.release_date, "vote_average": filme.vote_average})
            return filmes

        king = processar_resultados(king_filmes)
        originais = processar_resultados(outros_filmes)
        
        resultado_total = {
            "stephen_king": {"quantidade": len(king), "nota_media": sum(f["vote_average"] for f in king if f.get("vote_average")) / len(king) if king else 0, "filmes": king},
            "originais": {"quantidade": len(originais), "nota_media": sum(f["vote_average"] for f in originais if f.get("vote_average")) / len(originais) if originais else 0, "filmes": originais}
        }
    except Exception as e:
        print(f"Erro na análise de Stephen King: {e}")
        resultado_total = {"erro": str(e)}

    return resultado_total    

# Handler principal
def lambda_handler(event, context):    
    try:
        print(f"Modo desenvolvimento: {MODO_DESENVOLVIMENTO}")
        print(f"Iniciando coleta de dados para as análises...")
        
        # Executar análises uma por uma para debug
        print("\n1. Executando análise: Evolução da Protagonista Feminina...")
        dados_protagonista_feminina = analise_protagonista_feminina()
        processar_e_salvar_dados("protagonista_feminina", dados_protagonista_feminina)
        
        print("\n2. Executando análise: Ícones Masculinos do Terror Moderno...")
        dados_atores_terror = analise_atores_terror_moderno()
        processar_e_salvar_dados("atores_terror_moderno", dados_atores_terror)
        
        print("\n3. Executando análise: Terror Psicológico e Mistério...")
        dados_terror_psicologico = analise_terror_psicologico_misterio()
        processar_e_salvar_dados("terror_psicologico_misterio", dados_terror_psicologico)
        
        print("\n4. Executando análise: Dilema do Terror Leve...")
        dados_terror_classificacao = analise_terror_classificacao()
        processar_e_salvar_dados("terror_classificacao", dados_terror_classificacao)
        
        print("\n5. Executando análise: Mestres do Suspense Moderno...")
        dados_mestres_suspense = analise_mestres_suspense()
        processar_e_salvar_dados("mestres_suspense", dados_mestres_suspense)
        
        print("\n6. Executando análise: Toque de Midas de Stephen King...")
        dados_stephen_king = analise_stephen_king()
        processar_e_salvar_dados("stephen_king", dados_stephen_king)
        
        print("\nProcessamento concluído!")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Análises executadas com sucesso!')
        }
    
    except Exception as e:
        print(f"Erro na execução: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro: {str(e)}')
        }

# Para execução local
if __name__ == "__main__":
    os.environ['MODO_DESENVOLVIMENTO'] = 'True'
    
    print("Iniciando coleta de dados do TMDB...")
    print(f"TMDB_API_KEY: {'Configurada' if os.getenv('TMDB_API_KEY') else 'Não configurada'}")
    
    lambda_handler({}, {})