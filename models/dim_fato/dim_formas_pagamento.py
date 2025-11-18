# Responsável por: definir a estrutura da tabela dim_formas_pagamento no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Boolean, ForeignKey, UniqueConstraint
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - DIM_FORMAS_PAGAMENTO
# =====================================================

class DimFormasPagamento(Base):
    __tablename__ = "dim_formas_pagamento"
    __table_args__ = (
        UniqueConstraint('forma_pagamento_id', 'empresa_id', name='uq_dim_formas_pagamento_bling_empresa'),
        {"schema": "processed"}
    )

    # ============================
    # CHAVES
    # ============================
    
    # Chave primária (mesmo ID do Bling - SEM autoincrement)
    forma_pagamento_id = Column(BigInteger, primary_key=True)
    
    # Chave da empresa
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    
    # ============================
    # ATRIBUTOS DESCRITIVOS
    # ============================
    
    forma_pagamento = Column(String(255), nullable=False)
    
    # ============================
    # METADADOS
    # ============================
    
    data_ingestao = Column(DateTime, nullable=True)
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<DimFormasPagamento(forma_pagamento_id={self.forma_pagamento_id}, empresa_id={self.empresa_id}, forma_pagamento='{self.forma_pagamento}')>"