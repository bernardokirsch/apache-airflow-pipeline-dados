from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import pandas as pd
import json

# Caminhos base
BASE_PATH = "/opt/datalake"
BRONZE_PATH = os.path.join(BASE_PATH, "bronze")
SILVER_PATH = os.path.join(BASE_PATH, "silver")
GOLD_PATH = os.path.join(BASE_PATH, "gold")

# ======================
# ETAPA 1 - EXTRAÇÃO (BRONZE)
# ======================
def extract_data(**context):
    cnpjs = ["60133946000158"] # Lista de CNPJs para consulta
    data_list = []

    for cnpj in cnpjs:
        url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
        response = requests.get(url)
        response.raise_for_status()
        data_list.append(response.json())

    os.makedirs(BRONZE_PATH, exist_ok=True)
    bronze_file = os.path.join(BRONZE_PATH, "cnpj_raw.json")
    with open(bronze_file, "w", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=4)

    context['ti'].xcom_push(key='bronze_path', value=bronze_file)
    print(f"✅ Dados brutos salvos em: {bronze_file}")

# ======================
# ETAPA 2 - TRANSFORMAÇÃO (SILVER)
# ======================
def transform_data(**context):
    bronze_file = context['ti'].xcom_pull(key='bronze_path', task_ids='extract')
    with open(bronze_file, "r", encoding="utf-8") as f:
        data_list = json.load(f)

    df = pd.json_normalize(data_list)
    df = df[['cnpj', 'razao_social', 'nome_fantasia', 'descricao_situacao_cadastral', 'data_inicio_atividade', 'cnae_fiscal_descricao']]
    df.rename(columns={
        'cnpj': 'CNPJ',
        'razao_social': 'Razao_Social',
        'nome_fantasia': 'Nome_Fantasia',
        'descricao_situacao_cadastral': 'Situacao',
        'data_inicio_atividade': 'Data_Inicio_Atividade',
        'cnae_fiscal_descricao': 'Descricao_Atividade_Principal'
    }, inplace=True)
    df['Data_Atualizacao'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    os.makedirs(SILVER_PATH, exist_ok=True)
    silver_file = os.path.join(SILVER_PATH, "cnpj_clean.csv")
    df.to_csv(silver_file, index=False, encoding="utf-8-sig")

    context['ti'].xcom_push(key='silver_path', value=silver_file)
    print(f"✅ Dados transformados salvos em: {silver_file}")

# ======================
# ETAPA 3 - CARGA FINAL (GOLD)
# ======================
def load_data(**context):
    silver_file = context['ti'].xcom_pull(key='silver_path', task_ids='transform')
    df = pd.read_csv(silver_file)

    resumo = pd.DataFrame({
        "Total_Empresas": [len(df)],
        "Data_Geracao": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    })

    os.makedirs(GOLD_PATH, exist_ok=True)
    gold_file = os.path.join(GOLD_PATH, "cnpj_relatorio.csv")
    resumo.to_csv(gold_file, index=False, encoding="utf-8-sig")
    print(f"✅ Relatório final salvo em: {gold_file}")

# ======================
# DAG
# ======================
with DAG(
    dag_id='cnpj_query_etl_pipeline',
    description='ETL de CNPJs via BrasilAPI',
    schedule_interval='@monthly', # '@daily'
    start_date=datetime(2025, 11, 6),
    catchup=False,
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=2)},
    tags=["ETL", "API", "Pandas", "BrasilAPI"]
) as dag:

    extract = PythonOperator(task_id='extract', python_callable=extract_data)
    transform = PythonOperator(task_id='transform', python_callable=transform_data)
    load = PythonOperator(task_id='load', python_callable=load_data)

    extract >> transform >> load
