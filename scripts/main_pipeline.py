"""
Pipeline principal para processamento de dados de livros.

Este módulo orquestra a execução completa da pipeline de ETL,
incluindo limpeza de dados e feature engineering.
"""

import polars as pl
import time
import logging
from pathlib import Path
from typing import Dict, Any
from data_types import PipelineConfig, PipelineStats
from cleaning_pipeline import run_cleaning_pipeline
from feature_pipeline import run_feature_pipeline

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_directories(config: PipelineConfig) -> None:
    """
    Cria os diretórios necessários para a pipeline.
    
    Args:
        config: Configuração da pipeline
    """
    logger.info("Configurando diretórios...")
    
    directories = [
        Path(config.processed_output).parent,
        Path(config.features_output).parent
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Diretório criado/verificado: {directory}")


def validate_input_file(input_path: str) -> bool:
    """
    Valida se o arquivo de entrada existe e é válido.
    
    Args:
        input_path: Caminho do arquivo de entrada
        
    Returns:
        bool: True se válido, False caso contrário
    """
    logger.info(f"Validando arquivo de entrada: {input_path}")
    
    input_file = Path(input_path)
    
    if not input_file.exists():
        logger.error(f"Arquivo de entrada não encontrado: {input_path}")
        return False
    
    if not input_file.suffix.lower() == '.csv':
        logger.error(f"Arquivo deve ser CSV: {input_path}")
        return False
    
    try:
        # Tentar carregar e verificar se não está vazio
        df = pl.read_csv(input_path)
        if df.height == 0:
            logger.error("Arquivo de entrada está vazio!")
            return False
        
        logger.info(f"✅ Arquivo válido: {df.height} registros, {len(df.columns)} colunas")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao ler arquivo de entrada: {str(e)}")
        return False


def create_pipeline_stats(
    cleaning_stats: Dict,
    features_stats: Dict,
    execution_time: float
) -> PipelineStats:
    """
    Cria estatísticas consolidadas da pipeline.
    
    Args:
        cleaning_stats: Estatísticas da limpeza
        features_stats: Estatísticas das features
        execution_time: Tempo total de execução
        
    Returns:
        PipelineStats: Estatísticas consolidadas
    """
    return PipelineStats(
        total_records=cleaning_stats['total_records'],
        null_records_found=cleaning_stats['null_records_found'],
        duplicate_titles=0,  # Será implementado se necessário
        categories_cleaned=cleaning_stats['categories_cleaned'],
        processed_records=cleaning_stats['processed_records'],
        features_created=features_stats['features_created'],
        execution_time_seconds=execution_time
    )


def log_pipeline_summary(stats: PipelineStats, config: PipelineConfig) -> None:
    """
    Registra um resumo executivo da pipeline.
    
    Args:
        stats: Estatísticas da pipeline
        config: Configuração da pipeline
    """
    logger.info("=" * 60)
    logger.info("RESUMO EXECUTIVO DA PIPELINE")
    logger.info("=" * 60)
    logger.info(f"📊 Total de registros processados: {stats.total_records}")
    logger.info(f"🧹 Registros com nulos encontrados: {stats.null_records_found}")
    logger.info(f"🏷️  Categorias limpas: {stats.categories_cleaned}")
    logger.info(f"⚙️  Features criadas: {stats.features_created}")
    logger.info(f"⏱️  Tempo de execução: {stats.execution_time_seconds:.2f}s")
    logger.info(f"📁 Dados processados salvos em: {config.processed_output}")
    logger.info(f"🎯 Dados com features salvos em: {config.features_output}")
    logger.info("=" * 60)


def run_full_pipeline(config: PipelineConfig = None) -> PipelineStats:
    """
    Executa a pipeline completa de processamento de dados.
    
    Args:
        config: Configuração da pipeline (opcional, usa padrão se None)
        
    Returns:
        PipelineStats: Estatísticas da execução
        
    Raises:
        ValueError: Se houver erro na validação dos dados
        Exception: Para outros erros durante a execução
    """
    start_time = time.time()
    
    # Usar configuração padrão se não fornecida
    if config is None:
        config = PipelineConfig()
    
    logger.info("🚀 INICIANDO PIPELINE DE PROCESSAMENTO DE DADOS")
    logger.info(f"Configuração: {config.model_dump()}")
    
    try:
        # 1. Validar arquivo de entrada
        if not validate_input_file(config.input_file):
            raise ValueError(f"Arquivo de entrada inválido: {config.input_file}")
        
        # 2. Configurar diretórios
        setup_directories(config)
        
        # 3. Executar pipeline de limpeza
        logger.info("🧹 FASE 1: Pipeline de Limpeza")
        processed_df, cleaning_stats = run_cleaning_pipeline(
            config.input_file,
            config.processed_output,
            config
        )
        
        # 4. Executar pipeline de features
        logger.info("⚙️ FASE 2: Pipeline de Feature Engineering")
        features_df, features_stats = run_feature_pipeline(
            config.processed_output,
            config.features_output,
            config
        )
        
        # 5. Calcular estatísticas finais
        execution_time = time.time() - start_time
        final_stats = create_pipeline_stats(cleaning_stats, features_stats, execution_time)
        
        # 6. Log do resumo
        log_pipeline_summary(final_stats, config)
        
        logger.info("✅ PIPELINE CONCLUÍDA COM SUCESSO!")
        
        return final_stats
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"❌ ERRO NA PIPELINE após {execution_time:.2f}s: {str(e)}")
        raise


def run_cleaning_only(config: PipelineConfig = None) -> Dict:
    """
    Executa apenas a pipeline de limpeza.
    
    Args:
        config: Configuração da pipeline
        
    Returns:
        Dict: Estatísticas da limpeza
    """
    if config is None:
        config = PipelineConfig()
    
    logger.info("🧹 Executando apenas pipeline de limpeza...")
    
    if not validate_input_file(config.input_file):
        raise ValueError(f"Arquivo de entrada inválido: {config.input_file}")
    
    setup_directories(config)
    
    _, cleaning_stats = run_cleaning_pipeline(
        config.input_file,
        config.processed_output,
        config
    )
    
    logger.info("✅ Pipeline de limpeza concluída!")
    return cleaning_stats


def run_features_only(config: PipelineConfig = None) -> Dict:
    """
    Executa apenas a pipeline de feature engineering.
    Requer que os dados já tenham sido processados.
    
    Args:
        config: Configuração da pipeline
        
    Returns:
        Dict: Estatísticas das features
    """
    if config is None:
        config = PipelineConfig()
    
    logger.info("⚙️ Executando apenas pipeline de features...")
    
    if not Path(config.processed_output).exists():
        raise ValueError(f"Dados processados não encontrados: {config.processed_output}")
    
    setup_directories(config)
    
    _, features_stats = run_feature_pipeline(
        config.processed_output,
        config.features_output,
        config
    )
    
    logger.info("✅ Pipeline de features concluída!")
    return features_stats


if __name__ == "__main__":
    # Executar pipeline com configuração padrão
    try:
        stats = run_full_pipeline()
        print(f"Pipeline executada com sucesso em {stats.execution_time_seconds:.2f}s")
    except Exception as e:
        print(f"Erro na execução da pipeline: {str(e)}")
        exit(1)