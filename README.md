# Primeiros Passos com Apache Airflow: Construindo um Pipeline de Dados do Zero

Repositório referente aos códigos e documentações do **Minicurso “Primeiros Passos com Apache Airflow: Construindo um Pipeline de Dados do Zero”**.

---

## 📘 Visão Geral

O objetivo deste projeto é **demonstrar na prática os fundamentos do Apache Airflow**, ferramenta líder de orquestração de pipelines de dados.  
A proposta é construir um pipeline ETL **completo e automatizado**, entendendo o funcionamento real das engrenagens que sustentam projetos modernos de **Engenharia de Dados**.

### 🎯 Objetivos do Minicurso

1. Subir o **Apache Airflow** em containers via **Docker Compose**.  
2. Compreender os conceitos de **DAGs, Tasks e Operators**.  
3. Criar um **pipeline ETL completo**, capaz de:
   - **Extrair** informações de uma **API pública**.  
   - **Transformar** os dados utilizando **Python (Pandas)**.  
   - **Carregar** o resultado em um **arquivo CSV** organizado em um diretório de *datalake*.  

---

## 🧠 Arquitetura do Projeto

```shell
📁 apache-airflow-pipeline-dados
├── airflow/
│   ├── config/
│   ├── dags/
│   │   ├── dag_cnpj_query_etl_pipeline.py 
│   │   ├── dag_first_etl_pipeline.py 
│   │   ├── dag_ibge_estados_etl_pipeline.py
│   │   └── ... 
│   ├── logs/ 
│   └── plugins/                                     
├── datalake/ 
│   ├── bronze/ 
│   ├── silver/ 
│   └── gold/ 
├── .env 
├── docker-compose.yaml 
├── README.md
├── requirements.txt 
├── setup.ps1  # Script de inicialização (Windows)
└── setup.sh   # Script de inicialização (Linux/WSL)
```

### 📊 Estrutura do Datalake

- **Bronze:** dados brutos extraídos da API.  
- **Silver:** dados transformados e limpos (pós-Pandas).  
- **Gold:** relatórios e datasets prontos para consumo analítico.

---

## ⚙️ Tecnologias Utilizadas

| Categoria | Ferramenta |
|------------|-------------|
| Orquestração | **Apache Airflow 2.11** |
| Linguagem | **Python 3.11** |
| Containerização | **Docker + Docker Compose** |
| Transformação de Dados | **Pandas** |
| Requisições HTTP | **Requests** |
| Ambiente | **Windows (PowerShell) / Linux (WSL2)** |

---

## 🧩 Pré-requisitos

Certifique-se de ter os seguintes itens instalados:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Python 3.11+](https://www.python.org/downloads/)
- [Git](https://git-scm.com/downloads)
- **Windows PowerShell** ou **Linux Bash/WSL**

---

## ⚡ Setup Automático

Este projeto já possui scripts automatizados para inicialização do ambiente.

### 🪟 No Windows (PowerShell)

```powershell
# Clonar o repositório
git clone https://github.com/bernardokirsch/apache-airflow-pipeline-dados.git
cd apache-airflow-pipeline-dados

# Executar o setup automatizado
.\setup.ps1
```
