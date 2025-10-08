"""
Pipeline de limpeza de dados para livros.

Este módulo contém funções para realizar a limpeza básica dos dados,
incluindo verificação de nulos, criação de ID único, tratamento de 
categorias problemáticas e transformação da coluna availability.
"""

import polars as pl
import random
from pathlib import Path
import logging
from typing import Tuple
from data_types import PipelineConfig, validate_polars_dataframe, get_raw_data_schema

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_null_values(df: pl.DataFrame) -> Tuple[pl.DataFrame, int]:
    """
    Verifica e reporta valores nulos no DataFrame.
    
    Args:
        df: DataFrame Polars com os dados
        
    Returns:
        Tuple[pl.DataFrame, int]: DataFrame limpo e número de nulos encontrados
        
    Raises:
        ValueError: Se forem encontrados valores nulos nos dados
    """
    logger.info("Verificando valores nulos...")
    
    # Contar valores nulos por coluna
    null_counts = df.null_count()
    total_nulls = null_counts.sum_horizontal().sum()
    
    if total_nulls > 0:
        logger.error(f"Encontrados {total_nulls} valores nulos!")
        for col in df.columns:
            null_count = null_counts[col][0]
            if null_count > 0:
                logger.error(f"  {col}: {null_count} valores nulos")
        raise ValueError(f"Dataset contém {total_nulls} valores nulos. Limpeza necessária!")
    
    logger.info("✅ Nenhum valor nulo encontrado!")
    return df, 0


def create_unique_id(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cria uma coluna ID única como primeira coluna do dataset.
    
    Args:
        df: DataFrame Polars
        
    Returns:
        pl.DataFrame: DataFrame com coluna ID adicionada
    """
    logger.info("Criando IDs únicos...")
    
    # Criar IDs únicos usando números aleatórios
    n_rows = df.height
    ids = []
    used_ids = set()
    
    # Gerar IDs únicos aleatórios
    while len(ids) < n_rows:
        new_id = random.randint(100000, 999999)  # 6 dígitos
        if new_id not in used_ids:
            ids.append(new_id)
            used_ids.add(new_id)
    
    # Adicionar coluna ID como primeira coluna
    df_with_id = df.with_columns(
        pl.Series("id", ids)
    ).select(["id"] + [col for col in df.columns])
    
    # Verificar se todos os IDs são únicos
    unique_ids = df_with_id["id"].n_unique()
    if unique_ids != n_rows:
        logger.warning(f"IDs duplicados detectados! {unique_ids} únicos de {n_rows} total")
    
    logger.info(f"✅ {n_rows} IDs únicos aleatórios criados!")
    return df_with_id


def clean_categories(df: pl.DataFrame, config: PipelineConfig) -> Tuple[pl.DataFrame, int]:
    """
    Trata categorias problemáticas substituindo por uma categoria padrão.
    
    Args:
        df: DataFrame Polars
        config: Configuração da pipeline
        
    Returns:
        Tuple[pl.DataFrame, int]: DataFrame com categorias limpas e número de registros alterados
    """
    logger.info("Limpando categorias problemáticas...")
    
    # Contar registros com categorias problemáticas
    problematic_mask = pl.col('category').is_in(config.problematic_categories)
    problematic_count = df.filter(problematic_mask).height
    
    if problematic_count > 0:
        logger.info(f"Encontradas {problematic_count} categorias problemáticas:")
        for cat in config.problematic_categories:
            count = df.filter(pl.col('category') == cat).height
            if count > 0:
                logger.info(f"  '{cat}': {count} registros")
        
        # Substituir categorias problemáticas
        df_clean = df.with_columns(
            pl.when(pl.col('category').is_in(config.problematic_categories))
            .then(pl.lit(config.default_category))
            .otherwise(pl.col('category'))
            .alias('category')
        )
        
        logger.info(f"✅ {problematic_count} categorias substituídas por '{config.default_category}'")
    else:
        df_clean = df
        logger.info("✅ Nenhuma categoria problemática encontrada!")
    
    return df_clean, problematic_count


def transform_availability(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforma a coluna availability de string para binário (yes=1, else=0).
    
    Args:
        df: DataFrame Polars
        
    Returns:
        pl.DataFrame: DataFrame com availability transformada
    """
    logger.info("Transformando coluna availability...")
    
    # Verificar valores únicos antes da transformação
    unique_values = df['availability'].unique().to_list()
    logger.info(f"Valores únicos encontrados: {unique_values}")
    
    # Transformar para binário
    df_transformed = df.with_columns(
        pl.when(pl.col('availability').str.to_lowercase() == 'yes')
        .then(1)
        .otherwise(0)
        .alias('availability')
    )
    
    # Verificar resultado
    transformed_values = df_transformed['availability'].unique().to_list()
    count_yes = df_transformed.filter(pl.col('availability') == 1).height
    count_no = df_transformed.filter(pl.col('availability') == 0).height
    
    logger.info(f"✅ Availability transformada: {count_yes} 'yes' → 1, {count_no} outros → 0")
    
    return df_transformed


def validate_processed_data(df: pl.DataFrame) -> bool:
    """
    Valida os dados processados verificando tipos e constraints.
    
    Args:
        df: DataFrame processado
        
    Returns:
        bool: True se válido, False caso contrário
    """
    logger.info("Validando dados processados...")
    
    # Verificar se todas as colunas esperadas estão presentes
    expected_columns = [
        "id", "title", "price", "rating", "category", "image",
        "product_page", "availability", "stock", "image_base64"
    ]
    
    for col in expected_columns:
        if col not in df.columns:
            logger.error(f"Coluna obrigatória '{col}' não encontrada!")
            return False
    
    # Verificar tipos e constraints
    try:
        # ID deve ser único
        if df["id"].n_unique() != df.height:
            logger.error("IDs não são únicos!")
            return False
            
        # Preços devem ser positivos
        negative_prices = df.filter(pl.col('price') <= 0).height
        if negative_prices > 0:
            logger.error(f"{negative_prices} preços negativos ou zero encontrados!")
            return False
            
        # Ratings devem estar entre 1-5
        invalid_ratings = df.filter((pl.col('rating') < 1) | (pl.col('rating') > 5)).height
        if invalid_ratings > 0:
            logger.error(f"{invalid_ratings} ratings fora do range 1-5!")
            return False
            
        # Stock deve ser não-negativo
        negative_stock = df.filter(pl.col('stock') < 0).height
        if negative_stock > 0:
            logger.error(f"{negative_stock} stocks negativos encontrados!")
            return False
            
        # Availability deve ser 0 ou 1
        invalid_availability = df.filter(~pl.col('availability').is_in([0, 1])).height
        if invalid_availability > 0:
            logger.error(f"{invalid_availability} valores inválidos em availability!")
            return False
            
        logger.info("✅ Dados processados válidos!")
        return True
        
    except Exception as e:
        logger.error(f"Erro na validação: {str(e)}")
        return False


def run_cleaning_pipeline(input_path: str, output_path: str, config: PipelineConfig) -> Tuple[pl.DataFrame, dict]:
    """
    Executa a pipeline completa de limpeza de dados.
    
    Args:
        input_path: Caminho do arquivo de entrada
        output_path: Caminho do arquivo de saída
        config: Configuração da pipeline
        
    Returns:
        Tuple[pl.DataFrame, dict]: DataFrame processado e estatísticas
    """
    logger.info("Iniciando pipeline de limpeza...")
    logger.info(f"Entrada: {input_path}")
    logger.info(f"Saída: {output_path}")
    
    stats = {
        'total_records': 0,
        'null_records_found': 0,
        'categories_cleaned': 0,
        'processed_records': 0
    }
    
    try:
        # 1. Carregar dados brutos
        logger.info("Carregando dados brutos...")
        df = pl.read_csv(input_path)
        stats['total_records'] = df.height
        logger.info(f"Carregados {df.height} registros")
        
        # Validar schema dos dados brutos
        if not validate_polars_dataframe(df, get_raw_data_schema()):
            raise ValueError("Schema dos dados brutos inválido!")
        
        # 2. Verificar valores nulos
        df, null_count = check_null_values(df)
        stats['null_records_found'] = null_count
        
        # 3. Criar ID único
        df = create_unique_id(df)
        
        # 4. Limpar categorias
        df, categories_cleaned = clean_categories(df, config)
        stats['categories_cleaned'] = categories_cleaned
        
        # 5. Transformar availability
        df = transform_availability(df)
        
        # 6. Validar dados processados
        if not validate_processed_data(df):
            raise ValueError("Validação dos dados processados falhou!")
        
        # 7. Salvar dados processados
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        df.write_csv(output_path)
        stats['processed_records'] = df.height
        
        logger.info("✅ Pipeline de limpeza concluída!")
        logger.info(f"✅ Dados salvos em: {output_path}")
        logger.info(f"📊 Estatísticas: {stats}")
        
        return df, stats
        
    except Exception as e:
        logger.error(f"❌ Erro na pipeline de limpeza: {str(e)}")
        raise