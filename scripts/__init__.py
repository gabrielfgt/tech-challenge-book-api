"""
Package scripts para pipeline de processamento de dados de livros.

Este package contém todos os módulos necessários para executar
a pipeline de ETL completa, incluindo limpeza de dados e
feature engineering.
"""

from .data_types import PipelineConfig, PipelineStats
from .main_pipeline import run_full_pipeline, run_cleaning_only, run_features_only

__version__ = "1.0.0"
__author__ = "Tech Challenge Team"

__all__ = [
    "PipelineConfig",
    "PipelineStats", 
    "run_full_pipeline",
    "run_cleaning_only",
    "run_features_only"
]