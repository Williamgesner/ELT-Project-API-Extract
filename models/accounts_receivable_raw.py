# Responsável por: definir a estrutura da tabela contas_receber_raw

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - CONTAS A RECEBER (RAW)
# =====================================================

class ContasReceberRaw(Base):
    __table_args__ = {"schema": "raw"}
    __tablename__ = "contas_receber_raw"

    # Chave primária
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Chave de negócio (ID da API Bling)
    bling_id = Column(BigInteger, unique=True, nullable=False, index=True)
    
    # JSON completo da API (dados brutos)
    dados_json = Column(JSONB, nullable=False)
    
    # Controle de processamento
    data_ingestao = Column(DateTime, default=datetime.now)
    status_processamento = Column(String(20), default='pendente')

    def __repr__(self):
        return f"<ContasReceberRaw(bling_id={self.bling_id}, data_ingestao={self.data_ingestao})>"