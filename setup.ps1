# ================================================
# Script de setup do ambiente Airflow + Datalake
# Autor: Bernardo Gularte Kirsch
# ================================================

# Executar no Windows (PowerShell)
# .\setup.ps1

Write-Host "Iniciando setup do ambiente..." -ForegroundColor Cyan

# 1️⃣ Criação do ambiente virtual Python 3.11
if (-Not (Test-Path ".venv")) {
    Write-Host "Criando ambiente virtual (.venv)..."
    py -3.11 -m venv .venv
}
else {
    Write-Host "Ambiente virtual já existe, pulando..."
}

# 2️⃣ Ativar ambiente virtual
Write-Host "Ativando ambiente virtual..."
& .\.venv\Scripts\Activate.ps1

# 3️⃣ Instalar dependências do projeto
if (Test-Path "requirements.txt") {
    Write-Host "Instalando dependências..."
    pip install -r requirements.txt
}
else {
    Write-Host "Nenhum arquivo requirements.txt encontrado!"
}

# 4️⃣ Subir containers Docker
Write-Host "Subindo containers do Apache Airflow..."
docker compose up -d --remove-orphans

# 5️⃣ Criar diretórios do datalake
Write-Host "Criando estrutura de diretórios do datalake..."
$paths = @("./datalake", "./datalake/bronze", "./datalake/silver", "./datalake/gold")

foreach ($path in $paths) {
    if (-Not (Test-Path $path)) {
        New-Item -ItemType Directory -Path $path | Out-Null
        Write-Host "Criado: $path"
    }
    else {
        Write-Host "Já existe: $path"
    }
}

Write-Host "Setup concluído com sucesso!" -ForegroundColor Green