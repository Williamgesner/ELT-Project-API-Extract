# Responsável por: criar engine, sessão, base do SQLAlchemy

from sqlalchemy import create_engine, text # Biblioteca para se comunicar com meu Banco de Dados Postgre SQL
from sqlalchemy.orm import declarative_base, sessionmaker
from config.settings import database_url 

# =====================================================
# 1. CONFIGURAÇÃO DO BANCO DE DADOS
# =====================================================

# Cria o engine e a sessão do banco de dados
engine = create_engine(database_url)
Session = sessionmaker(bind=engine)

# Cria a base para os modelos SQLAlchemy
Base = declarative_base()

print("Modelo de dados definido !")

# =====================================================
# 2. FUNÇÕES AUXILIARES - Criando esquemas e tabelas
# =====================================================

# Cria o schema 'raw' se não existir
def create_schema_raw():
    print("Criando schema raw...")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()

# Cria as tabelas
def create_all_tables():
    print("Criando tabelas...")
    Base.metadata.create_all(engine)