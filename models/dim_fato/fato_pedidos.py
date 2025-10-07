# Responsável por: definir a estrutura da tabela fato_pedidos no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, Date, Numeric, DateTime, ForeignKey
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - FATO_PEDIDOS
# =====================================================

class FatoPedidos(Base):
    __table_args__ = {"schema": "processed"}
    __tablename__ = "fato_pedidos"

    # ============================
    # CHAVES
    # ============================
    
    # Chave primária
    pedido_id = Column(Integer, primary_key=True, autoincrement=True)
    # Chave de negócio (ID da API Bling)
    bling_pedido_id = Column(BigInteger, unique=True, nullable=False, index=True)
    # Número do pedido (visível para usuários)
    numero_pedido = Column(String(50), index=True)
    
    # ============================
    # CHAVES ESTRANGEIRAS
    # ============================
    
    # FK para dim_tempo (usando data como FK)
    data_pedido = Column(Date, ForeignKey('processed.dim_tempo.data_completa'), nullable=False, index=True)
    # FK para dim_contatos
    cliente_id = Column(Integer, ForeignKey('processed.dim_contatos.cliente_id'), index=True)
    # Canal de venda (por enquanto só o ID, depois vira FK)
    canal_id = Column(Integer, index=True)
    
    # ============================
    # MÉTRICAS FINANCEIRAS
    # ============================
    
    valor_total = Column(Numeric(15, 2), nullable=False)
    valor_frete = Column(Numeric(15, 2), default=0, nullable=False)
    
    # ============================
    # MÉTRICAS DE QUANTIDADE
    # ============================
    
    # Quantos tipos de produto diferentes no pedido
    quantidade_itens_total = Column(Integer, default=0)
    # Quantas unidades totais (soma das quantidades)
    quantidade_produtos_total = Column(Integer, default=0)
    
    # ============================
    # ATRIBUTOS DESCRITIVOS
    # ============================
    
    # Situação do pedido (texto: "Verificado", "Em aberto", etc.)
    situacao = Column(String(50), index=True)
    
    # ============================
    # METADADOS
    # ============================
    
    # Data de quando foi extraído da API
    data_ingestao = Column(DateTime)
    # Data de quando foi processado para o DW
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<FatoPedidos(pedido_id={self.pedido_id}, numero={self.numero_pedido}, valor={self.valor_total})>"