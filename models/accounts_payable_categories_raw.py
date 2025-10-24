# Responsável por: definir a estrutura da tabela categorias_raw

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - CATEGORIAS (RAW)
# =====================================================

class CategoriasRaw(Base):
    __table_args__ = {"schema": "raw"}
    __tablename__ = "categorias_contas_pagar_raw"

    # Chave primária
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Chave de negócio (ID da API Bling)
    bling_id = Column(BigInteger, unique=True, nullable=False, index=True)
    
    # JSON completo da API (dados brutos)
    dados_json = Column(JSONB, nullable=False)
    
    # Controle de processamento
    data_ingestao = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<CategoriasRaw(bling_categoria_id={self.bling_categoria_id}, data_ingestao={self.data_ingestao})>"