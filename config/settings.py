# Responsável por: carregar .env, validar variáveis, configurar API

import os
import pandas as pd
from dotenv import load_dotenv

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

# Validação das variáveis
if not all([postgres_username, postgres_password, postgres_host, postgres_port, postgres_database]):
    raise Exception("Variáveis do PostgreSQL não encontradas no .env")

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

# URLs de todos os endpoints da API Bling
endpoints = {
    'contatos': 'https://api.bling.com.br/Api/v3/contatos',
    'produtos': 'https://api.bling.com.br/Api/v3/produtos', 
    'vendas': 'https://api.bling.com.br/Api/v3/pedidos/vendas',
    'estoque': 'https://api.bling.com.br/Api/v3/estoques',  # Por hora não estamos usando esse endpoint. Mantido aqui para escalas futuras
    'situacoes': 'https://api.bling.com.br/Api/v3/situacoes',
    'canais': 'https://api.bling.com.br/Api/v3/canais-venda',

     # ===== DASHBOARD FINANCEIRO =====
    'contas_pagar': 'https://api.bling.com.br/Api/v3/contas/pagar',
    'contas_receber': 'https://api.bling.com.br/Api/v3/contas/receber',
    'categorias': 'https://api.bling.com.br/Api/v3/categorias/receitas-despesas',
    'nfe': 'https://api.bling.com.br/Api/v3/nfe',
    'formas_pagamentos': 'https://api.bling.com.br/Api/v3/formas-pagamentos',
    'natureza_operacao': 'https://api.bling.com.br/Api/v3/naturezas-operacoes'
}

# =====================================================
# 3. CONFIGURAÇÃO MULTI-CNPJ (CARREGA DO CSV)
# =====================================================

def carregar_empresas_do_csv():
    """
    Carrega as empresas do CSV e adiciona as API keys do .env (se existirem)
    
    MODIFICAÇÃO PARA AUTH_MANAGER:
    - Agora aceita empresas SEM API Key no .env
    - API Keys serão geradas pelo auth_manager quando necessário
    - Não levanta Exception se não encontrar API Keys
    """
    try:
        # Ler o CSV
        df = pd.read_csv('data_business/empresas.csv')
        
        empresas_list = []
        empresas_sem_api_key = []
        
        for _, row in df.iterrows():
            empresa_id = int(row['empresa_id'])
            
            # Buscar API key correspondente no .env
            api_key_var = f"API_KEY_{empresa_id:02d}"  # API_KEY_01, API_KEY_02, etc
            api_key = os.getenv(api_key_var)  # Busca no .env
            
            # 🆕 MUDANÇA: Não ignora mais empresas sem API Key!
            if not api_key:
                print(f"ℹ️  INFO: API Key '{api_key_var}' não encontrada no .env")
                print(f"   Empresa: {row['razao_social']}")
                print(f"   ✅ Será gerada automaticamente pelo auth_manager quando necessário")
                empresas_sem_api_key.append(empresa_id)
                # Define como None - será gerado depois
                api_key = None
            
            # Adiciona a empresa na lista (COM ou SEM API Key)
            empresas_list.append({
                'empresa_id': empresa_id,
                'nome': row['razao_social'],
                'cnpj': row['cnpj'],
                'api_key': api_key  # Pode ser None!
            })
        
        if not empresas_list:
            raise Exception(
                "Nenhuma empresa encontrada no CSV!\n"
                "Certifique-se de que o arquivo 'empresas.csv' existe e tem dados."
            )
        
        # Relatório de carregamento
        print(f"\n{'='*70}")
        print(f"📊 EMPRESAS CARREGADAS DO CSV:")
        print(f"{'='*70}")
        
        for emp in empresas_list:
            status = "✅ API Key presente" if emp['api_key'] else "⏳ API Key será gerada"
            print(f"   • ID {emp['empresa_id']}: {emp['nome']}")
            print(f"     └── {status}")
        
        if empresas_sem_api_key:
            print(f"\n💡 IMPORTANTE:")
            print(f"   • {len(empresas_sem_api_key)} empresa(s) sem API Key no .env")
            print(f"   • As chaves serão geradas automaticamente quando o pipeline rodar")
            print(f"   • Certifique-se de ter CLIENT_ID, CLIENT_SECRET e REFRESH_TOKEN configurados")
        
        print(f"{'='*70}\n")
        
        return empresas_list
        
    except FileNotFoundError:
        raise Exception(
            "Arquivo 'empresas.csv' não encontrado na raiz do projeto!\n"
            "Certifique-se de que o arquivo existe com as colunas: empresa_id, cnpj, razao_social"
        )
    except Exception as e:
        raise Exception(f"Erro ao carregar empresas do CSV: {e}")


# Carregar empresas automaticamente
empresas = carregar_empresas_do_csv()

print(f"✅ {len(empresas)} empresa(s) configurada(s)")

# =====================================================
# 4. HEADERS - MANTIDO VAZIO PARA COMPATIBILIDADE
# =====================================================
# IMPORTANTE: Este headers global NÃO é mais usado
# Cada extractor agora cria seu próprio headers com sua API key
# Mantido aqui apenas para não quebrar o base_extractor.py
headers = {}