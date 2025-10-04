"""
Pipeline de feature engineering para dados de livros.

Este módulo cria features derivadas dos dados processados, incluindo:
- price_range: Categorização de preços
- has_subtitle, has_series, starts_with_the: Features de título
- title_length: Comprimento do título
- One-hot encoding para categorias
- Rating e stock categorizados
- Feature de popularidade
"""

import polars as pl
import logging
from pathlib import Path
from typing import Tuple, Dict, List
from data_types import PipelineConfig, PriceRange, RatingCategory, StockLevel

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_price_range_feature(df: pl.DataFrame, config: PipelineConfig) -> pl.DataFrame:
    """
    Cria feature categórica de faixa de preços.
    
    Args:
        df: DataFrame com dados processados
        config: Configuração da pipeline
        
    Returns:
        pl.DataFrame: DataFrame com coluna price_range
    """
    logger.info("Criando feature price_range...")
    
    df_with_price_range = df.with_columns(
        pl.when(pl.col('price') <= 20).then(pl.lit(PriceRange.LOW.value))
        .when(pl.col('price') <= 40).then(pl.lit(PriceRange.MEDIUM.value))
        .when(pl.col('price') <= 50).then(pl.lit(PriceRange.HIGH.value))
        .otherwise(pl.lit(PriceRange.PREMIUM.value))
        .alias('price_range')
    )
    
    # Mostrar distribuição
    distribution = df_with_price_range['price_range'].value_counts().sort('count', descending=True)
    logger.info("Distribuição de faixas de preço:")
    for row in distribution.iter_rows():
        logger.info(f"  {row[0]}: {row[1]} livros")
    
    return df_with_price_range


def create_title_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cria features baseadas nos padrões dos títulos.
    
    Args:
        df: DataFrame com dados processados
        
    Returns:
        pl.DataFrame: DataFrame com features de título
    """
    logger.info("Criando features de título...")
    
    df_with_title_features = df.with_columns([
        # Has subtitle (contém ':')
        pl.col('title').str.contains(':').alias('has_subtitle'),
        
        # Has series (contém '(')
        pl.col('title').str.contains(r'\(').alias('has_series'),
        
        # Starts with 'The'
        pl.col('title').str.starts_with('The ').alias('starts_with_the'),
        
        # Title length
        pl.col('title').str.len_chars().alias('title_length'),
        
        # Title word count
        pl.col('title').str.split(' ').list.len().alias('title_word_count'),
        
        # Has numbers
        pl.col('title').str.contains(r'\d').alias('has_numbers')
    ])
    
    # Mostrar estatísticas
    stats = {
        'has_subtitle': df_with_title_features['has_subtitle'].sum(),
        'has_series': df_with_title_features['has_series'].sum(),
        'starts_with_the': df_with_title_features['starts_with_the'].sum(),
        'has_numbers': df_with_title_features['has_numbers'].sum(),
        'avg_title_length': df_with_title_features['title_length'].mean(),
        'avg_word_count': df_with_title_features['title_word_count'].mean()
    }
    
    logger.info("Estatísticas de features de título:")
    for feature, value in stats.items():
        if isinstance(value, float):
            logger.info(f"  {feature}: {value:.1f}")
        else:
            logger.info(f"  {feature}: {value}")
    
    return df_with_title_features


def create_rating_categories(df: pl.DataFrame) -> pl.DataFrame:
    """
    Categoriza ratings em grupos descritivos.
    
    Args:
        df: DataFrame com dados processados
        
    Returns:
        pl.DataFrame: DataFrame com rating_category
    """
    logger.info("Criando categorias de rating...")
    
    df_with_rating_cat = df.with_columns(
        pl.when(pl.col('rating') == 1).then(pl.lit(RatingCategory.VERY_LOW.value))
        .when(pl.col('rating') == 2).then(pl.lit(RatingCategory.LOW.value))
        .when(pl.col('rating') == 3).then(pl.lit(RatingCategory.MEDIUM.value))
        .when(pl.col('rating') == 4).then(pl.lit(RatingCategory.HIGH.value))
        .when(pl.col('rating') == 5).then(pl.lit(RatingCategory.VERY_HIGH.value))
        .alias('rating_category')
    )
    
    # Mostrar distribuição
    distribution = df_with_rating_cat['rating_category'].value_counts().sort('count', descending=True)
    logger.info("Distribuição de categorias de rating:")
    for row in distribution.iter_rows():
        logger.info(f"  {row[0]}: {row[1]} livros")
    
    return df_with_rating_cat


def create_stock_levels(df: pl.DataFrame) -> pl.DataFrame:
    """
    Categoriza stock em níveis (baixo/médio/alto).
    
    Args:
        df: DataFrame com dados processados
        
    Returns:
        pl.DataFrame: DataFrame com stock_level
    """
    logger.info("Criando níveis de stock...")
    
    df_with_stock_level = df.with_columns(
        pl.when(pl.col('stock') <= 5).then(pl.lit(StockLevel.LOW.value))
        .when(pl.col('stock') <= 15).then(pl.lit(StockLevel.MEDIUM.value))
        .otherwise(pl.lit(StockLevel.HIGH.value))
        .alias('stock_level')
    )
    
    # Mostrar distribuição
    distribution = df_with_stock_level['stock_level'].value_counts().sort('count', descending=True)
    logger.info("Distribuição de níveis de stock:")
    for row in distribution.iter_rows():
        logger.info(f"  {row[0]}: {row[1]} livros")
    
    return df_with_stock_level


def create_popularity_score(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cria score de popularidade baseado em rating e stock.
    
    Formula: (rating / 5) * 0.7 + (stock_normalized) * 0.3
    
    Args:
        df: DataFrame com dados processados
        
    Returns:
        pl.DataFrame: DataFrame com popularity_score
    """
    logger.info("Criando score de popularidade...")
    
    # Normalizar stock (0-1)
    max_stock = df['stock'].max()
    
    df_with_popularity = df.with_columns(
        (
            (pl.col('rating') / 5.0) * 0.7 +
            (pl.col('stock') / max_stock) * 0.3
        ).alias('popularity_score')
    )
    
    # Mostrar estatísticas
    stats = df_with_popularity['popularity_score'].describe()
    logger.info("Estatísticas de popularity_score:")
    logger.info(f"  Min: {df_with_popularity['popularity_score'].min():.3f}")
    logger.info(f"  Max: {df_with_popularity['popularity_score'].max():.3f}")
    logger.info(f"  Média: {df_with_popularity['popularity_score'].mean():.3f}")
    logger.info(f"  Mediana: {df_with_popularity['popularity_score'].median():.3f}")
    
    return df_with_popularity


def create_category_encoding(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cria one-hot encoding para categorias.
    
    Args:
        df: DataFrame com dados processados
        
    Returns:
        pl.DataFrame: DataFrame com colunas one-hot para categorias
    """
    logger.info("Criando one-hot encoding para categorias...")
    
    # Obter categorias únicas
    unique_categories = df['category'].unique().sort().to_list()
    logger.info(f"Encontradas {len(unique_categories)} categorias únicas")
    
    # Criar colunas one-hot
    category_columns = []
    for category in unique_categories:
        # Limpar nome da coluna (remover espaços e caracteres especiais)
        col_name = f"category_{category.replace(' ', '_').replace('&', 'and').lower()}"
        category_columns.append(
            pl.when(pl.col('category') == category)
            .then(1)
            .otherwise(0)
            .alias(col_name)
        )
    
    df_with_encoding = df.with_columns(category_columns)
    
    logger.info(f"Criadas {len(category_columns)} colunas de categoria:")
    for i, category in enumerate(unique_categories):
        col_name = f"category_{category.replace(' ', '_').replace('&', 'and').lower()}"
        count = df_with_encoding[col_name].sum()
        logger.info(f"  {col_name}: {count} livros")
    
    return df_with_encoding


def validate_features_data(df: pl.DataFrame) -> bool:
    """
    Valida se todas as features foram criadas corretamente.
    
    Args:
        df: DataFrame com features
        
    Returns:
        bool: True se válido, False caso contrário
    """
    logger.info("Validando features criadas...")
    
    required_features = [
        'price_range', 'has_subtitle', 'has_series', 'starts_with_the',
        'title_length', 'rating_category', 'stock_level', 
        'title_word_count', 'has_numbers', 'popularity_score'
    ]
    
    try:
        # Verificar se todas as features estão presentes
        for feature in required_features:
            if feature not in df.columns:
                logger.error(f"Feature obrigatória '{feature}' não encontrada!")
                return False
        
        # Validações específicas
        # title_length deve ser positivo
        if df.filter(pl.col('title_length') < 0).height > 0:
            logger.error("title_length contém valores negativos!")
            return False
        
        # popularity_score deve estar entre 0 e 1
        if df.filter((pl.col('popularity_score') < 0) | (pl.col('popularity_score') > 1)).height > 0:
            logger.error("popularity_score fora do range 0-1!")
            return False
        
        # Features booleanas devem ser True/False
        bool_features = ['has_subtitle', 'has_series', 'starts_with_the', 'has_numbers']
        for feature in bool_features:
            unique_values = df[feature].unique().to_list()
            if not all(isinstance(v, bool) for v in unique_values):
                logger.error(f"Feature booleana '{feature}' contém valores não-booleanos!")
                return False
        
        logger.info("✅ Todas as features válidas!")
        return True
        
    except Exception as e:
        logger.error(f"Erro na validação de features: {str(e)}")
        return False


def run_feature_pipeline(input_path: str, output_path: str, config: PipelineConfig) -> Tuple[pl.DataFrame, dict]:
    """
    Executa a pipeline completa de feature engineering.
    
    Args:
        input_path: Caminho do arquivo processado
        output_path: Caminho do arquivo com features
        config: Configuração da pipeline
        
    Returns:
        Tuple[pl.DataFrame, dict]: DataFrame com features e estatísticas
    """
    logger.info("Iniciando pipeline de feature engineering...")
    logger.info(f"Entrada: {input_path}")
    logger.info(f"Saída: {output_path}")
    
    stats = {
        'input_records': 0,
        'features_created': 0,
        'output_records': 0,
        'category_columns': 0
    }
    
    try:
        # 1. Carregar dados processados
        logger.info("Carregando dados processados...")
        df = pl.read_csv(input_path)
        stats['input_records'] = df.height
        logger.info(f"Carregados {df.height} registros")
        
        # 2. Criar features de preço
        df = create_price_range_feature(df, config)
        
        # 3. Criar features de título
        df = create_title_features(df)
        
        # 4. Criar categorias de rating
        df = create_rating_categories(df)
        
        # 5. Criar níveis de stock
        df = create_stock_levels(df)
        
        # 6. Criar score de popularidade
        df = create_popularity_score(df)
        
        # 7. Criar one-hot encoding para categorias
        original_cols = len(df.columns)
        df = create_category_encoding(df)
        stats['category_columns'] = len(df.columns) - original_cols
        
        # 8. Validar features
        if not validate_features_data(df):
            raise ValueError("Validação das features falhou!")
        
        # Contar features criadas
        base_columns = [
            "id", "title", "price", "rating", "category", "image",
            "product_page", "availability", "stock", "image_base64"
        ]
        stats['features_created'] = len(df.columns) - len(base_columns)
        
        # 9. Salvar dados com features
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        df.write_csv(output_path)
        stats['output_records'] = df.height
        
        logger.info("✅ Pipeline de feature engineering concluída!")
        logger.info(f"✅ Dados salvos em: {output_path}")
        logger.info(f"📊 Estatísticas: {stats}")
        
        return df, stats
        
    except Exception as e:
        logger.error(f"❌ Erro na pipeline de features: {str(e)}")
        raise