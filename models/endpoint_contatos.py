# Responsável por: definir a estrutura da tabela contatos_raw

from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, DateTime
from sqlalchemy.dialects.postgresql import JSONB  # Importa JSONB (Mais rápido e ja convertido)
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA
# =====================================================

# Definindo o modelo da tabela para dados brutos (raw)
class ContatoRaw(Base):
    __table_args__ = {"schema": "raw"} # Definindo o esquema 
    __tablename__ = "contatos_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bling_id = Column(BigInteger, unique=True, nullable=False) # ID original da API
    dados_json = Column(JSONB, nullable=False)  # JSONB é melhor que String para JSON. Nulllable é para dizer que a coluna não pode ser nula
    data_ingestao = Column(DateTime, default=datetime.now)  # Data de quando foi ingerido
    status_processamento = Column(String(20), default='pendente')  # Para controle para saber o que ja virou dim_clientes (na hora de processar)

    def __repr__(self):
        return f"<ContatoRaw(bling_id={self.bling_id}, data_ingestao={self.data_ingestao})>"