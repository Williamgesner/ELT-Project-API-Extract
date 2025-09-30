# Responsável por: definir a estrutura da tabela estoque_raw

from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, DateTime
from sqlalchemy.dialects.postgresql import JSONB  # Importa JSONB (Mais rápido e ja convertido)
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - ESTOQUE
# =====================================================

# Definindo o modelo da tabela para dados brutos (raw)
class EstoqueRaw(Base):
    __table_args__ = {"schema": "raw"} # Definindo o esquema 
    __tablename__ = "estoque_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bling_id = Column(BigInteger, unique=True, nullable=False) # ID original da API
    dados_json = Column(JSONB, nullable=False)  # JSONB é melhor que String para JSON. Nulllable é para dizer que a coluna não pode ser nula. Dados brutos do contato
    data_ingestao = Column(DateTime, default=datetime.now)  # Data de quando foi ingerido
    status_processamento = Column(String(20), default='pendente')  # Para controle de processamento - Saber o que ja virou dim_estoque (na hora de processar)

    def __repr__(self):
        return f"<EstoqueRaw(bling_id={self.bling_id}, data_ingestao={self.data_ingestao})>"