import gradio as gr
from google.cloud import bigquery
import openai
import re

# Defina as variáveis fixas
OPENAI_API_KEY = ""  # Substitua pela sua chave real
CREDENCIAIS_PATH = "pdm-class-2024-1-andre-5571fb20eb98.json"
PROJECT_ID = "pdm-class-2024-1-andre"

# Configurar OpenAI
def configurar_openai(api_key):
    openai.api_key = api_key

# Conectar ao BigQuery
def conectar_bigquery(credenciais_path, project_id):
    client = bigquery.Client.from_service_account_json(credenciais_path, project=project_id)
    return client

# Gerar consulta SQL
def gerar_consulta_sql(pergunta, modelo="gpt-4o-mini"):
    prompt = f"""
    Você é um assistente que traduz perguntas humanas em consultas SQL. A pergunta é: \"{pergunta}\".
    A tabela está localizada no BigQuery, no projeto `pdm-class-2024-1-andre`, dataset `dataset`, tabela `iot_data`.
    As colunas disponíveis são: Data, Hora, Planta, Sensor, Umidade_Solo, Temperatura_Ar, Umidade_Ar.
    Valores possíveis de plantas: Rúcula, Cebolinha, Couve e Alface Roxa.
    A coluna Hora é no seguinte formato 00:00:00. A coluna Sensor são valores inteiros.
    Colunas Umidade_Solo, Temperatura_Ar e Umidade_Ar são floats.
    Coluna Data no formato yyyy-mm-dd.
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

# Executar consulta no BigQuery
def executar_consulta(client, consulta_sql):
    query_job = client.query(consulta_sql)
    results = query_job.result()
    return results

# Processar a pergunta e executar a consulta SQL
def processar_pergunta(pergunta):
    configurar_openai(OPENAI_API_KEY)
    client = conectar_bigquery(CREDENCIAIS_PATH, PROJECT_ID)
    consulta_sql = gerar_consulta_sql(pergunta)
    results = executar_consulta(client, consulta_sql)

    resposta = []
    for row in results:
        resposta.append(dict(row))
    return resposta

# Gerar uma resposta final combinando SQL e GPT
def gerar_resposta_final(pergunta, resultados_sql):
    # Formatando os resultados da SQL como string
    contexto_sql = "\n".join([f"{chave}: {valor}" for resultado in resultados_sql for chave, valor in resultado.items()])
    contexto_sql = contexto_sql if contexto_sql else "Nenhum dado encontrado."

    # Usar o GPT com os resultados como contexto
    prompt = f"""
    A seguinte consulta SQL retornou os seguintes resultados:

    {contexto_sql}

    Baseado nesses resultados, responda à seguinte pergunta do usuário mas não cite que você fez uma consulta SQL: \"{pergunta}\"
    """
    resposta = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Você é um assistente que responde perguntas usando resultados SQL como contexto."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=300,
        temperature=0
    )
    resposta_final = resposta['choices'][0]['message']['content'].strip()
    return resposta_final

# Função principal da interface
def executar_interface(question):
    if not question:
        return "Por favor, faça uma pergunta."

    # Executar a consulta SQL
    resultados_sql = processar_pergunta(question)

    # Gerar a resposta final usando os resultados SQL como contexto
    resposta_final = gerar_resposta_final(question, resultados_sql)
    return resposta_final

# Construindo a interface Gradio
with gr.Blocks() as demo:

    gr.Image(value="image.png", show_label=False, width=150, height=150)
    gr.Markdown("# HidroSmart\n### Converse com seus dados!")

    question = gr.Textbox(label="Faça sua pergunta:", placeholder="Digite aqui sua pergunta...")
    output = gr.Textbox(label="Resposta final")  # Saída formatada

    btn = gr.Button("Executar consulta")
    btn.click(fn=executar_interface, inputs=question, outputs=output)

if __name__ == "__main__":
    demo.launch()
