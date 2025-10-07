# Responsável por: definir a estrutura da tabela situacoes_raw

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - SITUAÇÃO
# =====================================================

# Definindo o modelo da tabela para dados brutos (raw)
class SituacoesRaw(Base):
    __table_args__ = {"schema": "raw"}
    __tablename__ = "situacoes_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bling_situacao_id = Column(Integer, unique=True, nullable=False)  # ID original da API
    nome = Column(String(100), nullable=False)  # "Em aberto", "Verificado", etc.
    cor = Column(String(20))  # Cor hexadecimal (#E9DC40)
    dados_json = Column(JSONB, nullable=False)  # JSON completo da API
    data_ingestao = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<SituacoesRaw(id={self.bling_situacao_id}, nome='{self.nome}')>"