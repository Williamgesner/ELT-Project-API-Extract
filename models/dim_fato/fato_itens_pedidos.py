# Responsável por: definir a estrutura da tabela fato_itens_pedidos no schema processed

from datetime import datetime
import string
from tokenize import String
from sqlalchemy import Column, Integer, BigInteger, Numeric, DateTime, ForeignKey, String
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - FATO_ITENS_PEDIDOS
# =====================================================

class FatoItensPedidos(Base):
    __table_args__ = {"schema": "processed"}
    __tablename__ = "fato_itens_pedidos"

    # ============================
    # CHAVES
    # ============================
    
    # Chave primária
    item_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Chaves estrangeiras
    pedido_id = Column(Integer, ForeignKey('processed.fato_pedidos.pedido_id'), nullable=False, index=True)
    produto_id = Column(Integer, ForeignKey('processed.dim_produtos.produto_id'), index=True)
    
    # Chave de negócio (ID do item na API Bling)
    bling_item_id = Column(BigInteger, index=True)
    
    # ============================
    # MÉTRICAS DE QUANTIDADE
    # ============================
    
    quantidade = Column(Numeric(15, 3), nullable=False)  # Permite decimais (ex: 1.5 unidades)
    
    # ============================
    # MÉTRICAS FINANCEIRAS
    # ============================
    
    preco_unitario = Column(Numeric(15, 2), nullable=False)
    preco_total = Column(Numeric(15, 2), nullable=False)  # quantidade * preco_unitario
    
    # Descontos e acréscimos (se houver)
    desconto_valor = Column(Numeric(15, 2), default=0)
    
    # ============================
    # ATRIBUTOS DESCRITIVOS
    # ============================
    
    # Descrição do produto no momento da venda (pode diferir da dim_produtos)
    descricao_item = Column(String(500), nullable=True)
    
    # ============================
    # METADADOS
    # ============================
    
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<FatoItensPedidos(item_id={self.item_id}, pedido_id={self.pedido_id}, quantidade={self.quantidade}, valor={self.preco_total})>"