import os  # Esse modulo é usado para interagir com o sistema operacional
import requests
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv # Biblioteca para carregar as variáveis de ambiente
from sqlalchemy import create_engine # Biblioteca para se comunicar com meu Banco de Dados Postgre SQL
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB  # Importa JSONB (Mais rápido e ja convertido)
from sqlalchemy import (
    text,
    Column,
    Integer,
    String,
    BigInteger,
    Float,
    Numeric,
    DateTime,
    Boolean,
    ForeignKey,
)  # Atributos do banco de dados (tipos de colunas)

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
# 2. CONFIGURAÇÃO DO BANCO DE DADOS
# =====================================================

# Cria o engine e a sessão do banco de dados
engine = create_engine(database_url)
Session = sessionmaker(bind=engine)

# Cria a base para os modelos SQLAlchemy
Base = declarative_base()

# Definindo o modelo da tabela para dados brutos (raw)
class ContatoRaw(Base):
    __table_args__ = {"schema": "raw"} # Definindo o esquema 
    __tablename__ = "contatos_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bling_id = Column(BigInteger, unique=True, nullable=False) # ID original da API
    dados_json = Column(JSONB, nullable=False)  # JSONB é melhor que String para JSON. Nulllable é para dizer que a coluna não pode ser nula
    data_ingestao = Column(DateTime, default=datetime.now)  # Data quando foi ingerido
    status_processamento = Column(String(20), default='pendente')  # Para controle para saber o que ja virou dim_clientes (na hora de processar)

    def __repr__(self):
        return f"<ContatoRaw(bling_id={self.bling_id}, data_ingestao={self.data_ingestao})>"

print("Modelo de dados definido !")

# =====================================================
# 3. CONFIGURAÇÃO DA API BLING
# =====================================================

# Definindo os headers da requisição (Segundo a documentação da API)
headers = {
    "Authorization": f"Bearer {access_token}",  # Token OAuth obtido no fluxo
    "Content-Type": "application/json",
    "Accept": "application/json",
}
# Definindo os parametros da requisição (Segundo a documentação da API)
params = {
    "limite": 100,  # Máximo de registros por página (máx 100)
    "pagina": 1,  # Número da página (começa em 1)
}

# =====================================================
# 4. FUNÇÃO DE EXTRAÇÃO DOS DADOS
# =====================================================

# Definindo a função de extração e fazendo a requisição
def extract_dados_bling():
    url = "https://api.bling.com.br/Api/v3/contatos"
    response = requests.get(url, headers=headers, params=params)
    dados = response.json()
    contatos = dados["data"]
    return contatos

print(f"Iniciando extração de contatos...")

# =====================================================
# 5. FUNÇÃO PARA SALVAR NO POSTGRES
# =====================================================

# Função para salvar os dados no Postgres
def salvar_dados_postgres(dados):
    sessision = Session()
    novo_registro = ContatoRaw(**dados)
    sessision.add(novo_registro)
    sessision.commit()
    sessision.close()
    print(f"Dados salvos no Postgres")

# =====================================================
# 6. EXECUÇÃO DO SCRIPT (adicione no final)
# =====================================================

# Cria o schema se não existir
print("Criando schema raw...")
with engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
    conn.commit()

# Cria as tabelas
print("Criando tabelas...")
Base.metadata.create_all(engine)

# Extrai os dados da API
print("Extraindo contatos da API...")
contatos = extract_dados_bling()
print(f"Extraídos: {len(contatos)} contatos")

# Salva cada contato no banco
print("Salvando no banco...")
for contato in contatos:
    dados_para_salvar = {
        'bling_id': contato['id'],
        'dados_json': contato
    }
    salvar_dados_postgres(dados_para_salvar)

print("Concluído!")



# # Verificação
# if response.status_code == 200:


# df = pd.DataFrame(produtos)

# Alterar o nome da coluna id para id_contato
# df.columns = df.columns.str.replace('id', 'cliente_id')
# df.columns = df.columns.str.replace('numeroDocumento', 'cpf_cnpj')


# df= df.drop("codigo", axis=1) # o axis significa que a coluna sera removida
# df= df.drop("celular", axis=1)
# df= df.drop("situacao", axis=1)

# print(df.head())

# Baixar DF em CSV
# df.to_csv('dim_clientes.csv', index=False)
