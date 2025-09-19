import os  # Esse modulo é usado para interagir com o sistema operacional
import requests
import pandas as pd
import json
import time # Para adicionar delays entre requests se necessário
from datetime import datetime
from dotenv import load_dotenv # Biblioteca para carregar as variáveis de ambiente
from sqlalchemy import create_engine # Biblioteca para se comunicar com meu Banco de Dados Postgre SQL
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB  # Importa JSONB (Mais rápido e ja convertido)
from sqlalchemy.dialects.postgresql import insert
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

base_url = "https://api.bling.com.br/Api/v3/contatos"

# =====================================================
# 4. FUNÇÃO DE EXTRAÇÃO DOS DADOS
# =====================================================

# Definindo a função de extração e fazendo a requisição
def extract_dados_bling_paginado(limite_por_pagina=100, delay_entre_requests=0.5): # Extrai todos os contatos da API Bling usando paginação
    """
    Args   
        limite_por_pagina (int): Número máximo de registros por página (máx 100)
        delay_entre_requests (float): Tempo de espera entre requests em segundos

    Returns:
        list: Lista com todos os contatos extraídos
    """
    todos_contatos = []  # Lista para armazenar todos os contatos
    pagina_atual = 1     # Começamos da página 1
    total_paginas = None # Vamos descobrir isso na primeira requisição
    
    print(f"Iniciando extração paginada...")

    while True: # Loop infinito que vai quebrar quando não ouver mais páginas
        # Parâmetros para requisição
        params = {
            "limite": limite_por_pagina,
            "pagina": pagina_atual
        }

        print(f"Processando página {pagina_atual}{'/' + str(total_paginas) if total_paginas else ''}...")

        try:
            # Fazendo requisição para a API
            response = requests.get(base_url, headers=headers, params=params)
            # Verificando se a requisição foi bem sucedida
            if response.status_code != 200:
                print(f"Erro na requisição: {response.status_code} - {response.text}")
                break

            # Convertendo a resposta para JSON
            dados = response.json()

            # Na primeira requisição, capturamos o total de páginas
            if total_paginas is None:
                total_paginas = dados.get("total_pages", 1)
                total_registros = dados.get("total", 0)
                print(f"Total de páginas: {total_paginas}")
                print(f"Total de registros: {total_registros}")

            # Extraindo os contatos da página atual
            contatos_pagina = dados.get("data", [])
            
            # Se não há mais contatos, paramos o loop
            if not contatos_pagina:
                print(f"Página {pagina_atual} vazia. Finalizando extração.")
                break

            # Adicionando os contatos da página atual na lista principal
            todos_contatos.extend(contatos_pagina) # Extend é para adicionar os contatos da página atual na lista principal
            print(f"Extraídos {len(contatos_pagina)} contatos da página {pagina_atual}")
            
            # Se chegamos na última página, paramos o loop
            if pagina_atual >= total_paginas:
                print(f"Última página ({total_paginas}) processada. Finalizando.")
                break

            # Incrementamos para a próxima página
            pagina_atual += 1
            
            # Pausa entre requests para não sobrecarregar a API
            if delay_entre_requests > 0:
                time.sleep(delay_entre_requests)

        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição da página {pagina_atual}: {e}")
            break
        except KeyError as e:
            print(f"Erro ao processar dados da página {pagina_atual}: {e}")
            break
        except Exception as e:
            print(f"Erro inesperado na página {pagina_atual}: {e}")
            break
    
    print(f"Extração finalizada. Total de contatos coletados: {len(todos_contatos)}")
    return todos_contatos

# =====================================================
# 5. FUNÇÃO PARA SALVAR NO POSTGRES
# =====================================================

# Função para salvar os dados no Postgres
def salvar_dados_postgres_bulk(lista_dados): # Salva múltiplos contatos no Postgres de forma eficiente usando bulk insert
    
    if not lista_dados:
        print("Nenhum dado para salvar.")
        return
    
    session = Session()

    try:
        # Para cada dado na lista de dados, vamos salvar no Postgres
        for dados in lista_dados:
            stmt = insert(ContatoRaw).values(
                bling_id=dados['bling_id'],
                dados_json=dados['dados_json']
            )
            
            # Se já existir o mesmo bling_id → atualiza o JSON
            stmt = stmt.on_conflict_do_update(
                index_elements=['bling_id'],  # chave única
                set_={'dados_json': stmt.excluded.dados_json}
            )
            
            session.execute(stmt)

        session.commit()
        print(f"Upsert concluído! Total de {len(lista_dados)} registros processados.")
    except Exception as e:
        session.rollback()
        print(f"Erro ao salvar dados: {e}")
        raise
    finally:
        session.close()  # Sempre fechar a sessão do banco de dados


# =====================================================
# 6. EXECUÇÃO DO SCRIPT (adicione no final)
# =====================================================

# Cria o schema se não existir
if __name__ == "__main__": # Se o arquivo for executado diretamente, não execute o código abaixo. Isso é importante para evitar que o código seja executado quando o arquivo for importado.
    try:
        # Cria o schema se não existir
        print("Criando schema raw...")
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            conn.commit()

        # Cria as tabelas
        print("Criando tabelas...")
        Base.metadata.create_all(engine)

        # Extrai TODOS os dados da API usando paginação
        print("Extraindo todos os contatos da API...")
        todos_contatos = extract_dados_bling_paginado(
            limite_por_pagina=100,      # Máximo permitido pela API
            delay_entre_requests=0.5    # Meio segundo entre requests
        )

        if not todos_contatos:
            print("Nenhum contato foi extraído. Verificar API ou configurações.")
        else:
            # Preparando os dados para salvar no formato esperado
            print("Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for contato in todos_contatos:
                dados_formatados = {
                    'bling_id': contato['id'],
                    'dados_json': contato
                }
                dados_para_salvar.append(dados_formatados) # Append é para adicionar o dados formatados na lista principal
            
            # Salvar todos de uma vez (bulk)
            print("Salvando todos os contatos no banco Postgres...")

            salvar_dados_postgres_bulk(dados_para_salvar)

        print("Script executado com sucesso!")
        
    except Exception as e:
        print(f"Erro ao executar o script: {e}")
        raise