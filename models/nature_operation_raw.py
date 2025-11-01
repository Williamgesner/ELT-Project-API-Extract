# Responsável por: definir a estrutura da tabela natureza_operacao_raw

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - NATUREZA DE OPERAÇÃO
# =====================================================

# Definindo o modelo da tabela para dados brutos (raw)
class NaturezaOperacaoRaw(Base):
    __table_args__ = {"schema": "raw"}
    __tablename__ = "natureza_operacao_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bling_id = Column(BigInteger, unique=True, nullable=False)  # ID original da API
    dados_json = Column(JSONB, nullable=False)  # JSON completo da API
    data_ingestao = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<NaturezaOperacaoRaw(id={self.id}, bling_id={self.bling_id})>"