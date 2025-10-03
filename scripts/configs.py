"""Configurações centralizadas para o pipeline de dados.

Este módulo concentra caminhos, parâmetros e constantes reutilizadas
em múltiplos scripts para evitar duplicação e facilitar manutenção.
"""

from __future__ import annotations
from pathlib import Path
from typing import Final
import polars as pl

# Diretórios base
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
DATA_DIR: Final[Path] = PROJECT_ROOT / "data"
RAW_DATA_DIR: Final[Path] = DATA_DIR / "raw"
CLEANED_DATA_DIR: Final[Path] = DATA_DIR / "cleaned"
PROCESSED_DATA_DIR: Final[Path] = DATA_DIR / "processed"
FEATURES_DATA_DIR: Final[Path] = DATA_DIR / "features"
STATISTICS_DATA_DIR: Final[Path] = DATA_DIR / "statistics"

# Arquivos
RAW_BOOKS_FILENAME: Final[str] = "all_books_with_images.csv"
RAW_BOOKS_FILE: Final[Path] = RAW_DATA_DIR / RAW_BOOKS_FILENAME

# Colunas obrigatórias do dataset de livros
BOOKS_REQUIRED_COLUMNS: Final[list[str]] = [
	"title",
	"price",
	"rating",
	"availability",
	"category",
	"image",
	"product_page",
	"stock",
	"image_base64",
]

# Conjunto de dtypes numéricos suportados (evita uso de API inexistente como pl.datatypes.is_numeric)
NUMERIC_DTYPES = {
	pl.Int8, pl.Int16, pl.Int32, pl.Int64,
	pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
	pl.Float32, pl.Float64,
}

# Limiares e parâmetros de limpeza
NULL_THRESHOLD_BOOKS: Final[float] = 0.40  # 40%
UNKNOWN_FILL_VALUE: Final[str] = "unknown"
NUMERIC_IMPUTATION_STRATEGY: Final[str] = "median"  # (poderia ser mean, median, etc.)

# Nomes padrão de relatórios
NULLS_REPORT_FILENAME: Final[str] = "nulls_report_books_cleaned.csv"
DUPLICATES_REPORT_FILENAME: Final[str] = "duplicates_report_books_cleaned.csv"
CLEANED_BOOKS_FILENAME: Final[str] = "cleaned_books.csv"

# Transformer / processamento
PROCESSED_BOOKS_FILENAME: Final[str] = "processed_books.csv"
TEXT_NORMALIZE_COLUMNS: Final[list[str]] = ["title", "category"]
PRICE_COLUMN_NAME: Final[str] = "price"
STOCK_COLUMN_NAME: Final[str] = "stock"
TEXT_NORMALIZE_REPORT_FILENAME: Final[str] = "text_normalization_report.csv"
PRICE_TRANSFORM_REPORT_FILENAME: Final[str] = "price_transform_report.csv"
COLUMN_TYPES_REPORT_FILENAME: Final[str] = "column_types_report.csv"

# Feature engineering
FEATURES_FULL_FILENAME: Final[str] = "features_full.csv"
TRAIN_FEATURES_FILENAME: Final[str] = "features_train.csv"
TEST_FEATURES_FILENAME: Final[str] = "features_test.csv"
FEATURE_ENGINEERING_REPORT_SCALING: Final[str] = "feature_scaling_report.csv"
CATEGORICAL_ENCODING_REPORT_FILENAME: Final[str] = "categorical_encoding_report.csv"
EXTRA_FEATURES_REPORT_FILENAME: Final[str] = "extra_features_report.csv"
SPLIT_REPORT_FILENAME: Final[str] = "dataset_split_report.csv"

FEATURE_PRICE_MINMAX_COLUMN: Final[str] = PRICE_COLUMN_NAME  # coluna a escalar
FEATURE_TEST_SIZE: Final[float] = 0.3
FEATURE_RANDOM_SEED: Final[int] = 42
MAX_CATEGORICAL_CARDINALITY_OHE: Final[int] = 30  # se passar disso, sugerir outra técnica

# Outlier detection / feature engineering extras
OUTLIER_COLUMNS: Final[list[str]] = [PRICE_COLUMN_NAME, STOCK_COLUMN_NAME]  # colunas numéricas para detecção
OUTLIER_IQR_FACTOR: Final[float] = 1.5
OUTLIER_REPORT_FILENAME: Final[str] = "outlier_report.csv"

# Text features
TEXT_FEATURES_REPORT_FILENAME: Final[str] = "text_features_report.csv"
TEXT_TITLE_COLUMN: Final[str] = "title"
TEXT_CATEGORY_COLUMN: Final[str] = "category"

# Feature selection
FEATURE_SELECTION_VARIANCE_THRESHOLD: Final[float] = 0.0  # remove variância zero
FEATURE_SELECTION_K: Final[int] = 20  # máximo de features a manter via SelectKBest
FEATURE_SELECTION_REPORT_FILENAME: Final[str] = "feature_selection_report.csv"
FEATURE_SELECTION_TARGET: Final[str] = PRICE_COLUMN_NAME  # assumindo price como target para exemplo

# ID generation
ID_COLUMN_NAME: Final[str] = "id"
ID_REPORT_FILENAME: Final[str] = "id_generation_report.csv"
ID_DIGITS: Final[int] = 6  # 6 dígitos -> intervalo 100000-999999 (900000 combinações)

# EDA reports
EDA_CLEANED_PROFILE_FILENAME: Final[str] = "eda_cleaned_profile.csv"
EDA_PROCESSED_PROFILE_FILENAME: Final[str] = "eda_processed_profile.csv"
EDA_FEATURES_PROFILE_FILENAME: Final[str] = "eda_features_profile.csv"
EDA_CORRELATION_REPORT_FILENAME: Final[str] = "eda_features_correlations.csv"

# Garantir criação (não falha se já existir)
for _dir in [RAW_DATA_DIR, CLEANED_DATA_DIR, PROCESSED_DATA_DIR, FEATURES_DATA_DIR, STATISTICS_DATA_DIR]:
	_dir.mkdir(parents=True, exist_ok=True)

__all__ = [
	"PROJECT_ROOT",
	"DATA_DIR",
	"RAW_DATA_DIR",
	"CLEANED_DATA_DIR",
	"PROCESSED_DATA_DIR",
	"FEATURES_DATA_DIR",
	"STATISTICS_DATA_DIR",
	"RAW_BOOKS_FILE",
	"BOOKS_REQUIRED_COLUMNS",
	"NULL_THRESHOLD_BOOKS",
	"UNKNOWN_FILL_VALUE",
	"NUMERIC_IMPUTATION_STRATEGY",
	"NULLS_REPORT_FILENAME",
	"DUPLICATES_REPORT_FILENAME",
	"CLEANED_BOOKS_FILENAME",
	"PROCESSED_BOOKS_FILENAME",
	"TEXT_NORMALIZE_COLUMNS",
	"PRICE_COLUMN_NAME",
	"TEXT_NORMALIZE_REPORT_FILENAME",
	"PRICE_TRANSFORM_REPORT_FILENAME",
	"COLUMN_TYPES_REPORT_FILENAME",
	"ID_COLUMN_NAME",
	"ID_REPORT_FILENAME",
	"ID_DIGITS",
	"FEATURES_FULL_FILENAME",
	"TRAIN_FEATURES_FILENAME",
	"TEST_FEATURES_FILENAME",
	"FEATURE_ENGINEERING_REPORT_SCALING",
	"CATEGORICAL_ENCODING_REPORT_FILENAME",
	"EXTRA_FEATURES_REPORT_FILENAME",
	"SPLIT_REPORT_FILENAME",
	"FEATURE_PRICE_MINMAX_COLUMN",
	"FEATURE_TEST_SIZE",
	"FEATURE_RANDOM_SEED",
	"MAX_CATEGORICAL_CARDINALITY_OHE",
	"OUTLIER_COLUMNS",
	"OUTLIER_IQR_FACTOR",
	"OUTLIER_REPORT_FILENAME",
	"TEXT_FEATURES_REPORT_FILENAME",
	"TEXT_TITLE_COLUMN",
	"TEXT_CATEGORY_COLUMN",
	"FEATURE_SELECTION_VARIANCE_THRESHOLD",
	"FEATURE_SELECTION_K",
	"FEATURE_SELECTION_REPORT_FILENAME",
	"FEATURE_SELECTION_TARGET",
	"EDA_CLEANED_PROFILE_FILENAME",
	"EDA_PROCESSED_PROFILE_FILENAME",
	"EDA_FEATURES_PROFILE_FILENAME",
	"EDA_CORRELATION_REPORT_FILENAME",
	"NUMERIC_DTYPES",
	"STOCK_COLUMN_NAME",
]
