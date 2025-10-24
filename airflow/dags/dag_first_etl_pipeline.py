from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import json

# Caminhos base dentro do container
BASE_PATH = "/opt/datalake"
BRONZE_PATH = os.path.join(BASE_PATH, "bronze")
SILVER_PATH = os.path.join(BASE_PATH, "silver")
GOLD_PATH = os.path.join(BASE_PATH, "gold")

# ======================
# ETAPA 1 - EXTRAÇÃO (BRONZE)
# ======================

# Função de extração
def extract_data(**context):
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)
    response.raise_for_status()
    users = response.json()

    os.makedirs(BRONZE_PATH, exist_ok=True)
    bronze_file = os.path.join(BRONZE_PATH, "usuarios_raw.json")

    # Salva o JSON bruto
    with open(bronze_file, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=4)

    print(f"✅ Dados brutos salvos em: {bronze_file}")
    context['ti'].xcom_push(key='bronze_path', value=bronze_file)

# ======================
# ETAPA 2 - TRANSFORMAÇÃO (SILVER)
# ======================

# Função de transformação
def transform_data(**context):
    bronze_file = context['ti'].xcom_pull(key='bronze_path', task_ids='extract')

    with open(bronze_file, "r", encoding="utf-8") as f:
        users = json.load(f)

    df = pd.DataFrame(users)

    # Seleciona e renomeia colunas relevantes
    df = df[['id', 'name', 'username', 'email', 'phone', 'website']]
    df.rename(columns={
        'id': 'ID',
        'name': 'Nome',
        'username': 'Usuario',
        'email': 'Email',
        'phone': 'Telefone',
        'website': 'Site'
    }, inplace=True)

    # Adiciona timestamp de atualização
    df['Data_Atualizacao'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    os.makedirs(SILVER_PATH, exist_ok=True)
    silver_file = os.path.join(SILVER_PATH, "usuarios_clean.csv")
    df.to_csv(silver_file, index=False, encoding="utf-8-sig")

    print(f"✅ Dados transformados salvos em: {silver_file}")
    context['ti'].xcom_push(key='silver_path', value=silver_file)

# ======================
# ETAPA 3 - CARGA FINAL (GOLD)
# ======================

# Função de carga
def load_data(**context):
    silver_file = context['ti'].xcom_pull(key='silver_path', task_ids='transform')
    df = pd.read_csv(silver_file)

    # Agregação simples (quantidade de registros)
    resumo = pd.DataFrame({
        "Total_Usuarios": [len(df)],
        "Data_Geracao": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    })

    os.makedirs(GOLD_PATH, exist_ok=True)
    gold_file = os.path.join(GOLD_PATH, "usuarios_relatorio.csv")
    resumo.to_csv(gold_file, index=False, encoding="utf-8-sig")

    print(f"✅ Relatório final salvo em: {gold_file}")

# Definição da DAG
with DAG(
    dag_id='first_etl_pipeline',
    description='Pipeline ETL simples com Arquitetura Medalhão (Bronze, Silver, Gold)',
    schedule_interval='@monthly', # '@daily'
    start_date=datetime(2025, 11, 6),
    catchup=False,
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=2)},
    tags=['ETL', 'API', 'Pandas']
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_data
    )

    extract >> transform >> load
