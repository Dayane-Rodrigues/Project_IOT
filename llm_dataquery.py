import openai
import json
from google.cloud import bigquery

# Configuração da API da OpenAI
def configurar_openai(api_key):
    openai.api_key = api_key

# Configuração do cliente do BigQuery
def conectar_bigquery(credenciais_path, project_id):
    client = bigquery.Client.from_service_account_json(credenciais_path, project=project_id)
    return client

def gerar_consulta_sql(pergunta, modelo="gpt-4o-mini"):
    prompt = f"""
    Você é um assistente que traduz perguntas humanas em consultas SQL. A pergunta é: \"{pergunta}\".
    A tabela está localizada no BigQuery, no projeto `pdm-class-2024-1-andre`, dataset `dataset`, tabela `iot_data`.
    As colunas disponíveis são: Data, Hora, Planta, Sensor, Umidade_Solo, Temperatura_Ar, Umidade_Ar. 
    Valos possíveis de plantas: Rúcula, Cebolinha, Couve e Alface Roxa.
    A coluna Hora é no seguinte formato 00:00:00. A coluna Sensor são valores inteiros. 
    Colunas Umidade_Solo, Temperatura_Ar e Umidade_Ar são floats.
    Coluna data no formato yyyy-mm-dd.
    Sempre use o nome completo da tabela: `pdm-class-2024-1-andre.dataset.iot_data`. Escreva apenas a consulta SQL
    sem as aspas na resposta.
    """
    resposta = openai.ChatCompletion.create(
        model=modelo,
        messages=[
            {"role": "system", "content": "Você é um assistente especializado em SQL."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=200,
        temperature=0
    )

    sql_query = resposta['choices'][0]['message']['content'].strip()
    return sql_query


# Executar consulta SQL no BigQuery
def executar_consulta(client, consulta_sql):
    query_job = client.query(consulta_sql)
    results = query_job.result()
    return results

# Função principal para processar a pergunta e retornar os resultados
def processar_pergunta(pergunta, openai_api_key, credenciais_path, project_id):
    # Configurar OpenAI e BigQuery
    configurar_openai(openai_api_key)
    client = conectar_bigquery(credenciais_path, project_id)

    # Gerar a consulta SQL
    consulta_sql = gerar_consulta_sql(pergunta)
    print("Consulta SQL gerada:", consulta_sql)

    # Executar a consulta
    resultados = executar_consulta(client, consulta_sql)

    # Processar os resultados
    resposta = []
    for row in resultados:
        resposta.append(dict(row))

    return resposta

# Exemplo de uso
if __name__ == "__main__":
    openai_api_key = "chave_api"
    credenciais_path = "pdm-class-2024-1-andre-5571fb20eb98.json"
    project_id = "pdm-class-2024-1-andre"

    pergunta = "Qual foi a umidade média do solo para a planta couve?"

    resultados = processar_pergunta(pergunta, openai_api_key, credenciais_path, project_id)
    print("Resultados da consulta:", resultados)
