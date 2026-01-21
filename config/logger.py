"""
Sistema de logging para diagnóstico de performance
Salva todas as saídas do terminal em arquivo TXT com timestamps
"""

import sys
import os
from datetime import datetime
from pathlib import Path


class DualOutput:
    """
    Classe que duplica a saída: terminal + arquivo
    Mantém cores e formatação do terminal
    """
    
    def __init__(self, filepath):
        self.terminal = sys.stdout
        self.log = open(filepath, 'w', encoding='utf-8')
        
    def write(self, message):
        # Escreve no terminal (com cores)
        self.terminal.write(message)
        
        # Remove códigos ANSI de cor antes de salvar no arquivo
        clean_message = self._remove_ansi_codes(message)
        self.log.write(clean_message)
        
    def flush(self):
        # Força gravação imediata (importante!)
        self.terminal.flush()
        self.log.flush()
        
    def close(self):
        self.log.close()
        
    @staticmethod
    def _remove_ansi_codes(text):
        """
        Remove códigos de cor ANSI do texto
        (para arquivo ficar limpo)
        """
        import re
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        return ansi_escape.sub('', text)


def setup_logging(empresa_id, execution_number=None):
    """
    Configura sistema de logging para salvar todas as saídas
    
    Args:
        empresa_id: ID da empresa sendo processada
        execution_number: Número da execução (1, 2, 3...) - opcional
    
    Returns:
        str: Caminho do arquivo de log criado
    """
    
    # Criar pasta de logs se não existir
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Nome do arquivo com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if execution_number:
        filename = f"pipeline_empresa{empresa_id}_exec{execution_number}_{timestamp}.txt"
    else:
        filename = f"pipeline_empresa{empresa_id}_{timestamp}.txt"
    
    filepath = log_dir / filename
    
    # Redirecionar stdout e stderr para o arquivo + terminal
    sys.stdout = DualOutput(filepath)
    sys.stderr = DualOutput(filepath)
    
    # Header do log
    print("=" * 80)
    print(f"🔍 LOG DE DIAGNÓSTICO - EMPRESA {empresa_id}")
    print("=" * 80)
    print(f"📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if execution_number:
        print(f"🔢 Execução: #{execution_number}")
    print(f"📁 Arquivo de log: {filepath}")
    print("=" * 80)
    print()
    
    return str(filepath)


def close_logging():
    """
    Fecha o sistema de logging e restaura saída padrão
    """
    if isinstance(sys.stdout, DualOutput):
        print()
        print("=" * 80)
        print(f"🏁 LOG FINALIZADO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        sys.stdout.close()
        sys.stdout = sys.stdout.terminal
        
    if isinstance(sys.stderr, DualOutput):
        sys.stderr.close()
        sys.stderr = sys.stderr.terminal


def log_timestamp(message):
    """
    Imprime mensagem com timestamp preciso
    Útil para medir tempo entre etapas
    """
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] {message}")