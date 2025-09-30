# Responsável por: criar engine, sessão, base do SQLAlchemy

from sqlalchemy import create_engine, text # Biblioteca para se comunicar com meu Banco de Dados Postgre SQL
from sqlalchemy.orm import declarative_base, sessionmaker
from config.settings import database_url 


# =====================================================
# 2. CONFIGURAÇÃO DO BANCO DE DADOS
# =====================================================

# Cria o engine e a sessão do banco de dados
engine = create_engine(database_url)
Session = sessionmaker(bind=engine)

# Cria a base para os modelos SQLAlchemy
Base = declarative_base()

print("Modelo de dados definido !")

# =====================================================
# 3. FUNÇÕES AUXILIARES - Criando esquemas e tabelas
# =====================================================

# Cria o schema 'raw' se não existir
def create_schema_raw():
    print("Criando schema raw...")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()

# Cria as tabelas
def create_all_tables():
    print("Criando todas as tabelas...")
    print("Importando modelos:")

    # =====================================================
    # 1. IMPORTAÇÃO DE TODOS OS MODELOS
    # =====================================================

    # Importando aqui DENTRO da função, para evita a importação circular
    # IMPORTANTE: Importar todos os modelos para que o SQLAlchemy os reconheça e possa criar as tabelas automaticamente
    from models.contact_raw import ContatoRaw
    from models.product_raw import ProdutoRaw
    from models.sales_raw import VendasRaw
    from models.stocks_raw import EstoqueRaw

    print("Todos os modelos importados!")
    print("Tabelas que serão criadas:")
    print("- raw.contatos_raw")
    print("- raw.produtos_raw") 
    print("- raw.vendas_raw")
    print("- raw.estoque_raw")
    
    Base.metadata.create_all(engine)
    print("✅ Todas as tabelas foram criadas com sucesso!")