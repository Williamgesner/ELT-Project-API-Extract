# Responsável por: definir a estrutura da tabela dim_empresas no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - DIM_EMPRESAS
# =====================================================

class DimEmpresas(Base):
    __table_args__ = {"schema": "processed"}
    __tablename__ = "dim_empresas"

    # ============================
    # CHAVE PRIMÁRIA
    # ============================
    
    empresa_id = Column(Integer, primary_key=True)  # PK manual (1, 2, 3, 4, 5)
    
    # ============================
    # DADOS DA EMPRESA
    # ============================
    
    cnpj = Column(String(14), unique=True, nullable=False, index=True)  # 14 dígitos
    razao_social = Column(String(255), nullable=False)
    
    # ============================
    # METADADOS
    # ============================
    
    data_ingestao = Column(DateTime, default=datetime.now, nullable=False)
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<DimEmpresas(empresa_id={self.empresa_id}, cnpj='{self.cnpj}', razao_social='{self.razao_social}')>"