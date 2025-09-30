# Responsável por: carregar .env, validar variáveis, configurar API

import os # Esse modulo é usado para interagir com o sistema operacional
from dotenv import load_dotenv # Biblioteca para carregar as variáveis de ambiente

# =====================================================
# 1. CONFIGURAÇÃO DE AMBIENTE
# =====================================================

print("Carregando configurações...")

# Carregando as variáveis de ambiente do arquivo .env
load_dotenv()
postgres_username = os.getenv("postgres_username")
postgres_password = os.getenv("postgres_password")
postgres_host = os.getenv("postgres_host")
postgres_port = os.getenv("postgres_port")
postgres_database = os.getenv("postgres_database")

# Configurando a API_KEY BLING
access_token = os.getenv("API_KEY")

# Validação das variáveis
if not all([postgres_username, postgres_password, postgres_host, postgres_port, postgres_database]):
    raise Exception("Variáveis do PostgreSQL não encontradas no .env")

if not access_token:
    raise Exception("API_KEY não encontrada no .env")

# Construção da URL do banco
database_url = (
    f"postgresql://{postgres_username}:{postgres_password}"
    f"@{postgres_host}:{postgres_port}/{postgres_database}"
)

print(f"Configurações carregadas")
print(f"Banco: {postgres_host}:{postgres_port}/{postgres_database}")

# =====================================================
# 2. CONFIGURAÇÃO DA API BLING - TODOS OS ENDPOINTS
# =====================================================

# Definindo os headers da requisição (Segundo a documentação da API)
headers = {
    "Authorization": f"Bearer {access_token}",  # Token OAuth obtido no fluxo
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# URLs de todos os endpoints da API Bling
endpoints = {
    'contatos': 'https://api.bling.com.br/Api/v3/contatos',
    'produtos': 'https://api.bling.com.br/Api/v3/produtos', 
    'vendas': 'https://api.bling.com.br/Api/v3/pedidos/vendas',
    'estoque': 'https://api.bling.com.br/Api/v3/estoques'
}