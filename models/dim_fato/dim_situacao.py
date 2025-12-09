# Responsável por: definir a estrutura da tabela dim_situacao no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, UniqueConstraint
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - DIM_SITUACAO
# =====================================================

class DimSituacao(Base):
    __tablename__ = "dim_situacao"
    __table_args__ = (
        UniqueConstraint('bling_situacao_id', 'empresa_id', name='uq_dim_situacao_bling_empresa'),
        {"schema": "processed"}
    )

    # Chave primária
    id = Column(Integer, primary_key=True, autoincrement=True)  # ID interno (mesmo do raw)
    
    # Chave de negócio
    bling_situacao_id = Column(Integer, nullable=False, index=True)  # ID da API Bling
    
    # Chave da empresa
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    
    # Dados da situação
    situacao = Column(String(100), nullable=False, index=True)  # Nome da situação (Ex: "Em aberto", "Atendido")
    
    # Metadados
    data_ingestao = Column(DateTime, nullable=True)  # Data de quando foi extraído da API
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)  # Data de quando foi processado

    def __repr__(self):
        return f"<DimSituacao(bling_id={self.bling_situacao_id}, empresa_id={self.empresa_id}, situacao='{self.situacao}')>"