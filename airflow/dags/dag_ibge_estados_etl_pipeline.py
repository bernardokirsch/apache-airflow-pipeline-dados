from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import os
import requests
import pandas as pd
import json

# ======================
# CONFIGURAÇÕES
# ======================
BASE_PATH = "/opt/datalake"
BRONZE_PATH = os.path.join(BASE_PATH, "bronze")
SILVER_PATH = os.path.join(BASE_PATH, "silver")
GOLD_PATH = os.path.join(BASE_PATH, "gold")

# ======================
# ETAPA 1 - EXTRAÇÃO (BRONZE)
# ======================
def extract_data(**context):
    url = "https://brasilapi.com.br/api/ibge/uf/v1"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    os.makedirs(BRONZE_PATH, exist_ok=True)
    bronze_file = os.path.join(BRONZE_PATH, "ibge_estados_raw.json")

    with open(bronze_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    context["ti"].xcom_push(key="bronze_path", value=bronze_file)
    print(f"✅ Dados brutos salvos em: {bronze_file}")

# ======================
# ETAPA 2 - TRANSFORMAÇÃO (SILVER)
# ======================
def transform_data(**context):
    bronze_file = context["ti"].xcom_pull(key="bronze_path", task_ids="extract")
    with open(bronze_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    df = df[["id", "sigla", "nome", "regiao.nome"]]
    df.rename(columns={
        "id": "Codigo_IBGE",
        "sigla": "UF",
        "nome": "Estado",
        "regiao.nome": "Regiao"
    }, inplace=True)

    df["Data_Processamento"] = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M:%S")

    os.makedirs(SILVER_PATH, exist_ok=True)
    silver_file = os.path.join(SILVER_PATH, "ibge_estados_clean.csv")
    df.to_csv(silver_file, index=False, encoding="utf-8-sig")

    context["ti"].xcom_push(key="silver_path", value=silver_file)
    print(f"✅ Dados transformados salvos em: {silver_file}")

# ======================
# ETAPA 3 - CARGA FINAL (GOLD)
# ======================
def load_data(**context):
    silver_file = context["ti"].xcom_pull(key="silver_path", task_ids="transform")
    df = pd.read_csv(silver_file)

    resumo = df.groupby("Regiao").agg(
        Total_Estados=("Estado", "count")
    ).reset_index()

    resumo["Data_Geracao"] = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M:%S")

    os.makedirs(GOLD_PATH, exist_ok=True)
    gold_file = os.path.join(GOLD_PATH, "ibge_estados_resumo.csv")
    resumo.to_csv(gold_file, index=False, encoding="utf-8-sig")

    print(f"✅ Relatório final salvo em: {gold_file}")

# ======================
# DAG
# ======================
with DAG(
    dag_id="ibge_estados_etl_pipeline",
    description="ETL dos estados brasileiros via BrasilAPI (IBGE)",
    schedule_interval='@monthly', # '@daily'
    start_date=datetime(2025, 11, 6),
    catchup=False,
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=1)},
    tags=["ETL", "API", "Pandas", "BrasilAPI"]
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=extract_data)
    transform = PythonOperator(task_id="transform", python_callable=transform_data)
    load = PythonOperator(task_id="load", python_callable=load_data)

    extract >> transform >> load
