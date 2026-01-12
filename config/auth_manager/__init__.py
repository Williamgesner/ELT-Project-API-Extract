# config/auth_manager/__init__.py

"""
Módulo de Autenticação Automática da API Bling
"""

from .auth_manager import obter_token_para_empresa, BlingAuthManager

__all__ = ['obter_token_para_empresa', 'BlingAuthManager']