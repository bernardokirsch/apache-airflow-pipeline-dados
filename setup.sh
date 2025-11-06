#!/bin/bash
set -e

# ================================================
# Script de setup do ambiente Airflow + Datalake
# Autor: Bernardo Gularte Kirsch
# ================================================

# Executar no Linux (Bash)
# bash setup.sh

# 0️⃣ Atualizando pacotes do sistema e instalando dependências básicas
echo "🔄 Atualizando pacotes do sistema..."

sudo apt update
sudo apt install -y python3.11-venv python3.11-distutils

echo "🚀 Iniciando setup do ambiente..."

# 1️⃣ Criar ambiente virtual Python 3.11
if [ ! -d ".venv" ]; then
    echo "🧱 Criando ambiente virtual (.venv)..."
    PYTHON=$(command -v python3.11 || command -v python3)
    $PYTHON -m venv .venv
else
    echo "✅ Ambiente virtual já existe, pulando..."
fi

# 2️⃣ Ativar ambiente virtual
echo "🔗 Ativando ambiente virtual..."
source .venv/bin/activate

# 3️⃣ Instalar dependências
if [ -f "requirements.txt" ]; then
    echo "📦 Instalando dependências..."
    pip install -r requirements.txt
else
    echo "⚠️ Nenhum arquivo requirements.txt encontrado!"
fi

# 4️⃣ Subir containers Docker
echo "🐳 Subindo containers do Apache Airflow..."
docker compose up -d --remove-orphans

# 5️⃣ Criar diretórios do datalake
echo "🗂️ Criando estrutura de diretórios do datalake..."
mkdir -p ./datalake/bronze ./datalake/silver ./datalake/gold

echo "✅ Setup concluído com sucesso!"
