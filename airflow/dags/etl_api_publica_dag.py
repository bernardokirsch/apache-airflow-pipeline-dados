from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import os

# Caminho base para salvar os dados dentro do container
DATA_PATH = "/opt/datalake/bronze"

# Função de extração
def extract_data(**context):
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)
    response.raise_for_status()
    users = response.json()
    context['ti'].xcom_push(key='raw_data', value=users)

# Função de transformação
def transform_data(**context):
    users = context['ti'].xcom_pull(key='raw_data', task_ids='extract')
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

    context['ti'].xcom_push(key='transformed_data', value=df.to_dict(orient='records'))

# Função de carga
def load_data(**context):
    data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
    df = pd.DataFrame(data)

    # Garante que o diretório existe
    os.makedirs(DATA_PATH, exist_ok=True)

    output_path = os.path.join(DATA_PATH, "usuarios_relatorio.csv")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"✅ Arquivo salvo com sucesso em: {output_path}")

# Definição da DAG
with DAG(
    dag_id='etl_api_publica_dag',
    description='Pipeline ETL simples: API pública → Pandas → CSV',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={'retries': 1},
    tags=['ETL', 'API', 'Pandas']
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        provide_context=True
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True
    )

    extract >> transform >> load
