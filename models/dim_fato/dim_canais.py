# Responsável por: definir a estrutura da tabela dim_canais no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - DIM_CANAIS
# =====================================================

class DimCanais(Base):
    __table_args__ = {"schema": "processed"}
    __tablename__ = "dim_canais"
    
    # Chave de negócio
    bling_canal_id = Column(Integer, unique=True, nullable=False, index=True)
    
    # Atributos descritivos
    nome_canal = Column(String(200), nullable=False)
    
    # Metadados
    data_ingestao = Column(DateTime)
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<DimCanais(canal_id={self.canal_id}, nome_canal='{self.nome_canal}')>"