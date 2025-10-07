# Responsável por: definir a estrutura da tabela dim_tempo no schema processed

from sqlalchemy import Column, Integer, String, Date, Boolean
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - DIM_TEMPO
# =====================================================

class DimTempo(Base):
    __table_args__ = {"schema": "processed"}
    __tablename__ = "dim_tempo"

    # Chave primária - A DATA em si
    data_completa = Column(Date, primary_key=True)
    
    # Atributos da data
    ano = Column(Integer, nullable=False)
    mes = Column(Integer, nullable=False)
    dia = Column(Integer, nullable=False)
    trimestre = Column(Integer, nullable=False)
    semestre = Column(Integer, nullable=False)
    
    # Nomes
    nome_mes = Column(String(20), nullable=False)  # Janeiro, Fevereiro, etc.
    nome_mes_abrev = Column(String(3), nullable=False)  # Jan, Fev, etc.
    
    # Dia da semana
    dia_semana = Column(Integer, nullable=False)  # 0=Segunda, 6=Domingo
    nome_dia_semana = Column(String(20), nullable=False)  # Segunda-feira, etc.
    nome_dia_semana_abrev = Column(String(3), nullable=False)  # Seg, Ter, etc.
    
    # Flags booleanas
    eh_fim_semana = Column(Boolean, default=False, nullable=False)
    eh_feriado = Column(Boolean, default=False, nullable=False)
    
    # Semana do ano
    semana_ano = Column(Integer)
    
    def __repr__(self):
        return f"<DimTempo(data={self.data_completa}, ano={self.ano}, mes={self.mes})>"