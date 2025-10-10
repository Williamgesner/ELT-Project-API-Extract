# Responsável por: definir a estrutura da tabela canais_raw

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - CANAIS DE VENDA
# =====================================================

# Definindo o modelo da tabela para dados brutos (raw)
class CanaisRaw(Base):
    __table_args__ = {"schema": "raw"}
    __tablename__ = "canais_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bling_canal_id = Column(Integer, unique=True, nullable=False)  # ID original da API
    descricao = Column(String(200), nullable=False)  # "Loja Virtual", "Marketplace", etc.
    dados_json = Column(JSONB, nullable=False)  # JSON completo da API
    data_ingestao = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<CanaisRaw(id={self.bling_canal_id}, descricao='{self.descricao}')>"