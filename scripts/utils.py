"""Utilitários comuns reutilizados pelos scripts do pipeline de dados.

Este módulo concentra funções utilitárias para evitar duplicação de código
entre os diferentes scripts de processamento de dados.
"""

from __future__ import annotations
from pathlib import Path
from typing import Optional, List, Dict, Any
import polars as pl
import numpy as np
import re
from dataclasses import dataclass


@dataclass
class DataFrameValidationResult:
    """Resultado da validação de um DataFrame."""
    is_valid: bool
    error_message: Optional[str] = None
    missing_columns: Optional[List[str]] = None


class DataFrameValidator:
    """Classe para validações comuns de DataFrames."""
    
    @staticmethod
    def validate_not_none(df: Optional[pl.DataFrame], context: str = "DataFrame") -> None:
        """Valida se o DataFrame não é None."""
        if df is None:
            raise ValueError(f"{context} não carregado. Execute o método de leitura primeiro.")
    
    @staticmethod
    def validate_required_columns(
        df: pl.DataFrame, 
        required_columns: List[str],
        raise_on_missing: bool = False
    ) -> DataFrameValidationResult:
        """
        Valida se o DataFrame possui as colunas obrigatórias.
        
        Args:
            df: DataFrame a ser validado
            required_columns: Lista de colunas obrigatórias
            raise_on_missing: Se True, levanta exceção quando houver colunas ausentes
            
        Returns:
            Resultado da validação com informações sobre colunas ausentes
        """
        if df is None:
            return DataFrameValidationResult(False, "DataFrame é None")
            
        existing_columns = set(df.columns)
        missing_columns = [col for col in required_columns if col not in existing_columns]
        
        if missing_columns:
            error_msg = f"Colunas ausentes: {missing_columns}"
            if raise_on_missing:
                raise ValueError(error_msg)
            return DataFrameValidationResult(False, error_msg, missing_columns)
            
        return DataFrameValidationResult(True)


class DirectoryManager:
    """Classe para gerenciamento de diretórios."""
    
    @staticmethod
    def ensure_directories(*dirs: Path) -> None:
        """Garante que os diretórios existam, criando-os se necessário."""
        for directory in dirs:
            directory.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def validate_file_exists(file_path: Path, context: str = "Arquivo") -> None:
        """Valida se um arquivo existe."""
        if not file_path.exists():
            raise FileNotFoundError(f"{context} não encontrado: {file_path}")


class ReportGenerator:
    """Classe para geração de relatórios padronizados."""
    
    @staticmethod
    def save_report(
        report_data: List[Dict[str, Any]], 
        output_path: Path,
        context: str = "relatório"
    ) -> Path:
        """
        Salva dados de relatório em CSV.
        
        Args:
            report_data: Lista de dicionários com dados do relatório
            output_path: Caminho onde salvar o arquivo
            context: Contexto para mensagens de erro
            
        Returns:
            Caminho do arquivo salvo
        """
        if not report_data:
            report_data = [{"status": f"no_data_for_{context}"}]
            
        report_df = pl.DataFrame(report_data)
        DirectoryManager.ensure_directories(output_path.parent)
        report_df.write_csv(output_path)
        return output_path
    
    @staticmethod
    def create_column_report(
        columns: List[str], 
        dtypes: List[Any],
        mapper_func: callable = None
    ) -> List[Dict[str, str]]:
        """
        Cria relatório padronizado para colunas e tipos.
        
        Args:
            columns: Lista de nomes de colunas
            dtypes: Lista de tipos das colunas
            mapper_func: Função opcional para mapear tipos
            
        Returns:
            Lista de dicionários com informações das colunas
        """
        report_data = []
        for col, dtype in zip(columns, dtypes):
            mapped_type = mapper_func(dtype) if mapper_func else str(dtype)
            report_data.append({
                "column": col,
                "dtype": str(dtype),
                "mapped_type": mapped_type,
            })
        return report_data


class DataTypeHelper:
    """Classe com helpers para tipos de dados."""
    
    @staticmethod
    def get_numeric_dtypes() -> set:
        """Retorna conjunto de tipos numéricos do Polars."""
        return {
            pl.Int8, pl.Int16, pl.Int32, pl.Int64,
            pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
            pl.Float32, pl.Float64
        }
    
    @staticmethod
    def get_string_dtypes() -> set:
        """Retorna conjunto de tipos string do Polars."""
        return {pl.Utf8, pl.Categorical}
    
    @staticmethod
    def is_numeric_column(df: pl.DataFrame, column: str) -> bool:
        """Verifica se uma coluna é numérica."""
        if column not in df.columns:
            return False
        return df.dtypes[df.columns.index(column)] in DataTypeHelper.get_numeric_dtypes()
    
    @staticmethod
    def is_string_column(df: pl.DataFrame, column: str) -> bool:
        """Verifica se uma coluna é string/categórica."""
        if column not in df.columns:
            return False
        return df.dtypes[df.columns.index(column)] in DataTypeHelper.get_string_dtypes()
    
    @staticmethod
    def map_dtype_to_category(dtype: Any) -> str:
        """Mapeia dtype Polars para categoria semântica."""
        if dtype in DataTypeHelper.get_numeric_dtypes():
            return "numeric"
        if dtype in DataTypeHelper.get_string_dtypes():
            return "categorical"
        if dtype == pl.Boolean:
            return "boolean"
        return "other"


class TextProcessor:
    """Classe para processamento de texto."""
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Normaliza texto: lowercase, underscores, remove símbolos.
        
        Args:
            text: Texto a ser normalizado
            
        Returns:
            Texto normalizado
        """
        if not isinstance(text, str) or not text:
            return ""
            
        # Converte para lowercase
        normalized = text.lower()
        
        # Remove caracteres especiais mantendo apenas alfanuméricos
        pattern_non_alnum = re.compile(r"[^a-z0-9]+")
        normalized = pattern_non_alnum.sub("_", normalized)
        
        # Remove underscores no início e fim
        normalized = normalized.strip("_")
        
        # Remove múltiplos underscores consecutivos
        normalized = re.sub(r"__+", "_", normalized)
        
        return normalized
    
    @staticmethod
    def safe_len(text: Any) -> int:
        """Retorna comprimento seguro de texto."""
        return len(text) if isinstance(text, str) else 0
    
    @staticmethod
    def word_count(text: Any) -> int:
        """Conta palavras em texto de forma segura."""
        return len(text.split()) if isinstance(text, str) else 0
    
    @staticmethod
    def has_number(text: Any) -> bool:
        """Verifica se texto contém dígitos."""
        return any(ch.isdigit() for ch in text) if isinstance(text, str) else False
    
    @staticmethod
    def token_count(text: Any, separator: str = "_") -> int:
        """Conta tokens separados por um caractere."""
        return len(text.split(separator)) if isinstance(text, str) else 0


class NumericProcessor:
    """Classe para processamento de dados numéricos."""
    
    @staticmethod
    def parse_price(value: Any) -> Optional[float]:
        """
        Parse de valores de preço com limpeza de formatação.
        
        Args:
            value: Valor a ser convertido
            
        Returns:
            Valor convertido para float ou None se não for possível
        """
        if value is None:
            return None
            
        if not isinstance(value, str):
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        # Remove espaços
        cleaned = value.strip()
        
        # Remove todos os espaços
        cleaned = re.sub(r"[\s]", "", cleaned)
        
        # Mantém apenas dígitos, vírgulas e pontos
        cleaned = re.sub(r"[^0-9,\.]", "", cleaned)
        
        if not cleaned:
            return None
        
        # Trata vírgula e ponto
        if "," in cleaned and "." in cleaned:
            # Se vírgula vem depois do ponto, assume vírgula como decimal
            if cleaned.find(",") > cleaned.find("."):
                cleaned = cleaned.replace(".", "")
            cleaned = cleaned.replace(",", ".")
        elif "," in cleaned:
            # Apenas vírgula -> assume decimal
            cleaned = cleaned.replace(",", ".")
        
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def calculate_outlier_bounds(
        series: pl.Series, 
        iqr_factor: float = 1.5
    ) -> Optional[tuple[float, float]]:
        """
        Calcula limites para detecção de outliers usando IQR.
        
        Args:
            series: Série numérica
            iqr_factor: Fator multiplicador do IQR
            
        Returns:
            Tupla (limite_inferior, limite_superior) ou None se não for possível calcular
        """
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        
        if q1 is None or q3 is None:
            return None
            
        iqr = q3 - q1
        if iqr == 0:
            return None
            
        lower = q1 - iqr_factor * iqr
        upper = q3 + iqr_factor * iqr
        
        return (lower, upper)
    
    @staticmethod
    def safe_correlation(
        arr1: np.ndarray, 
        arr2: np.ndarray
    ) -> Optional[float]:
        """
        Calcula correlação de forma segura, tratando casos especiais.
        
        Args:
            arr1: Primeiro array
            arr2: Segundo array
            
        Returns:
            Coeficiente de correlação ou None se não for possível calcular
        """
        # Remove NaNs
        mask = ~np.isnan(arr1) & ~np.isnan(arr2)
        
        if mask.sum() < 2:
            return None
            
        clean_arr1 = arr1[mask]
        clean_arr2 = arr2[mask]
        
        # Verifica variância zero
        if np.std(clean_arr1) == 0 or np.std(clean_arr2) == 0:
            return 0.0
            
        try:
            return float(np.corrcoef(clean_arr1, clean_arr2)[0, 1])
        except (ValueError, RuntimeWarning):
            return None


class IdGenerator:
    """Classe para geração de IDs únicos."""
    
    @staticmethod
    def generate_unique_ids(
        count: int, 
        digits: int = 6, 
        seed: Optional[int] = None
    ) -> List[int]:
        """
        Gera lista de IDs únicos aleatórios.
        
        Args:
            count: Quantidade de IDs a gerar
            digits: Número de dígitos dos IDs
            seed: Seed para reprodutibilidade
            
        Returns:
            Lista de IDs únicos
        """
        import random
        
        if seed is not None:
            random.seed(seed)
        
        low = 10 ** (digits - 1)
        high = (10 ** digits) - 1
        capacity = high - low + 1
        
        if count > capacity:
            raise ValueError(
                f"Número de IDs solicitados ({count}) excede capacidade "
                f"({capacity}) para {digits} dígitos."
            )
        
        return random.sample(range(low, high + 1), count)


class ColumnSelector:
    """Classe para seleção de colunas baseada em critérios."""
    
    @staticmethod
    def get_numeric_columns(df: pl.DataFrame) -> List[str]:
        """Retorna lista de colunas numéricas."""
        numeric_dtypes = DataTypeHelper.get_numeric_dtypes()
        return [col for col, dtype in zip(df.columns, df.dtypes) if dtype in numeric_dtypes]
    
    @staticmethod
    def get_categorical_columns(df: pl.DataFrame) -> List[str]:
        """Retorna lista de colunas categóricas."""
        string_dtypes = DataTypeHelper.get_string_dtypes()
        return [col for col, dtype in zip(df.columns, df.dtypes) if dtype in string_dtypes]
    
    @staticmethod
    def filter_by_cardinality(
        df: pl.DataFrame, 
        columns: List[str], 
        max_cardinality: int
    ) -> List[tuple[str, int]]:
        """
        Filtra colunas categóricas por cardinalidade máxima.
        
        Args:
            df: DataFrame
            columns: Lista de colunas para filtrar
            max_cardinality: Cardinalidade máxima
            
        Returns:
            Lista de tuplas (coluna, cardinalidade) para colunas que passaram no filtro
        """
        filtered = []
        for col in columns:
            if col in df.columns:
                cardinality = df.select(pl.col(col).n_unique()).item()
                if cardinality <= max_cardinality:
                    filtered.append((col, cardinality))
        return filtered


# Constantes úteis exportadas
NUMERIC_DTYPES = DataTypeHelper.get_numeric_dtypes()
STRING_DTYPES = DataTypeHelper.get_string_dtypes()