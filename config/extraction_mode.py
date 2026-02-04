# Responsável por: gerenciar modos de extração (FULL vs INCREMENTAL)

from datetime import datetime, timedelta
from enum import Enum

class ExtractionMode(Enum):
    """Modos de extração disponíveis"""
    FULL = "full"
    INCREMENTAL = "incremental"