from __future__ import annotations
from pathlib import Path
from typing import Optional, List, Dict
import polars as pl

# Configurações centralizadas
from scripts.configs import (
	RAW_BOOKS_FILE,
	STATISTICS_DATA_DIR as STATISTICS_DIR,
	CLEANED_DATA_DIR as CLEANED_DIR,
	BOOKS_REQUIRED_COLUMNS,
	NULL_THRESHOLD_BOOKS,
	UNKNOWN_FILL_VALUE,
	NUMERIC_IMPUTATION_STRATEGY,
	NULLS_REPORT_FILENAME,
	DUPLICATES_REPORT_FILENAME,
	CLEANED_BOOKS_FILENAME,
	NUMERIC_DTYPES
)


class DataCleaner:
	"""Classe para limpeza e preparação dos dados de livros.

	Etapas:
	1. Leitura do arquivo raw (Polars)
	2. Validação de colunas obrigatórias
	3. Tratamento de valores nulos (remover coluna ou imputar)
	4. Remoção de duplicidades
	5. Salvamento de dataset limpo e estatísticas
	"""

	REQUIRED_COLUMNS = BOOKS_REQUIRED_COLUMNS
	NULL_THRESHOLD = NULL_THRESHOLD_BOOKS

	def __init__(self, raw_file: Path = RAW_BOOKS_FILE):
		self.raw_file = raw_file
		self.df: Optional[pl.DataFrame] = None
		self.nulls_report: Optional[pl.DataFrame] = None
		self.duplicates_report: Optional[pl.DataFrame] = None
		self.missing_columns: List[str] = []

		# Garante diretórios necessários
		STATISTICS_DIR.mkdir(parents=True, exist_ok=True)
		CLEANED_DIR.mkdir(parents=True, exist_ok=True)

	# 1. Carrega o arquivo raw
	def read_raw(self) -> pl.DataFrame:
		"""Lê o CSV bruto e carrega em self.df."""
		if not self.raw_file.exists():
			raise FileNotFoundError(f"Arquivo não encontrado: {self.raw_file}")
		self.df = pl.read_csv(self.raw_file)
		return self.df

	# 2. Valida colunas obrigatórias
	def validate_columns(self) -> None:
		"""Verifica colunas obrigatórias e registra ausentes sem abortar."""
		if self.df is None:
			raise ValueError("DataFrame não carregado. Chame read_raw() primeiro.")
		existing = set(self.df.columns)
		self.missing_columns = [c for c in self.REQUIRED_COLUMNS if c not in existing]
		if self.missing_columns:
			print(f"[WARN] Colunas ausentes: {self.missing_columns}. Prosseguindo sem elas.")


	# 3. Trata valores nulos
	def handle_nulls(self, filename: str = NULLS_REPORT_FILENAME) -> pl.DataFrame:
		"""Imputa ou remove colunas conforme percentual de nulos e gera relatório."""
		if self.df is None:
			raise ValueError("DataFrame não carregado. Chame read_raw() primeiro.")

		total_rows = self.df.height
		report_rows: List[Dict[str, str]] = []

		df_processed = self.df

		for col in self.df.columns:
			series = df_processed[col]
			null_count = series.null_count()
			null_pct = null_count / total_rows if total_rows else 0
			dtype = series.dtype
			action = "none"
			strategy = ""  # para imputação

			if null_count > 0:
				if null_pct >= self.NULL_THRESHOLD:
					# Excluir coluna inteira
					df_processed = df_processed.drop(col)
					action = "column_dropped"
				else:
					# Imputação por tipo de dado
					if dtype in NUMERIC_DTYPES:
						if NUMERIC_IMPUTATION_STRATEGY == "median":
							value = series.median()
							strategy = "median"
						elif NUMERIC_IMPUTATION_STRATEGY == "mean":
							value = series.mean()
							strategy = "mean"
						else:
							value = series.median()
							strategy = "median_default"
					elif dtype == pl.Utf8 or dtype == pl.Categorical:
						value = UNKNOWN_FILL_VALUE
						strategy = f"constant_{UNKNOWN_FILL_VALUE}"
					else:
						value = UNKNOWN_FILL_VALUE
						strategy = f"constant_{UNKNOWN_FILL_VALUE}"

					df_processed = df_processed.with_columns(
						pl.when(pl.col(col).is_null()).then(value).otherwise(pl.col(col)).alias(col)
					)
					action = "imputed"

			report_rows.append(
				{
					"column": col,
					"type": str(dtype),
					"nulls": str(null_count),
					"pct_nulls": f"{null_pct:.2%}",
					"action": action,
					"strategy": strategy,
				}
			)

		# Adiciona linhas para colunas ausentes (não existiam no dataset)
		for missing_col in getattr(self, "missing_columns", []):
			report_rows.append(
				{
					"column": missing_col,
					"type": "MISSING",
					"nulls": "N/A",
					"pct_nulls": "100% (missing)",
					"action": "missing_not_created",
					"strategy": "none",
				}
			)

		self.df = df_processed
		self.nulls_report = pl.DataFrame(report_rows)

		# Salvar relatório de nulos
		nulls_path = STATISTICS_DIR / filename
		self.nulls_report.write_csv(nulls_path)
		return self.df

	# 4. Remover duplicidades
	def remove_duplicates(self, filename: str = DUPLICATES_REPORT_FILENAME) -> pl.DataFrame:
		"""Elimina linhas duplicadas pelas colunas obrigatórias presentes e reporta estatísticas."""
		if self.df is None:
			raise ValueError("DataFrame não carregado. Chame read_raw() primeiro.")

		rows_before = self.df.height
		# Considerar duplicidade pelo conjunto de colunas obrigatórias presentes
		subset = [c for c in self.REQUIRED_COLUMNS if c in self.df.columns]
		deduped = self.df.unique(subset=subset, keep="first")
		rows_after = deduped.height
		removed_rows = rows_before - rows_after

		self.df = deduped
		self.duplicates_report = pl.DataFrame([
			{
				"rows_before": rows_before,
				"rows_after": rows_after,
				"duplicates_removed": removed_rows,
				"pct_removed": f"{(removed_rows / rows_before):.2%}" if rows_before else "0%",
			}
		])
		dup_path = STATISTICS_DIR / filename
		self.duplicates_report.write_csv(dup_path)
		return self.df

	# 5. Salvar dataset limpo
	def save_cleaned(self, filename: str = CLEANED_BOOKS_FILENAME) -> Path:
		"""Salva DataFrame final limpo em CSV e retorna o caminho."""
		if self.df is None:
			raise ValueError("DataFrame não carregado.")
		out_path = CLEANED_DIR / filename
		self.df.write_csv(out_path)
		return out_path

	def run(self) -> Path:
		"""Executa a sequência: read -> valida -> nulos -> duplicados -> salva."""
		self.read_raw()
		self.validate_columns()
		self.handle_nulls()
		self.remove_duplicates()
		cleaned_path = self.save_cleaned()
		return cleaned_path


if __name__ == "__main__":
	cleaner = DataCleaner()
	path = cleaner.run()
	print(f"Dataset limpo salvo em: {path}")
