# Responsável por: definir a estrutura da tabela dim_natureza_operacao

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, DateTime
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - DIMENSÃO NATUREZA DE OPERAÇÃO
# =====================================================

class DimNaturezaOperacao(Base):
    __table_args__ = {"schema": "processed"}
    __tablename__ = "dim_natureza_operacao"

    natureza_operacao_id = Column(Integer, primary_key=True, autoincrement=True)
    bling_natureza_operacao_id = Column(BigInteger, unique=True, nullable=False)  # ID original da API
    natureza_operacao = Column(String(255), nullable=False)  # Descrição
    data_ingestao = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<DimNaturezaOperacao(id={self.natureza_operacao_id}, natureza='{self.natureza_operacao}')>"