from __future__ import annotations
import sys
from pathlib import Path
from typing import Optional, List, Dict
import re
import polars as pl

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
try:
	from scripts.configs import (  # type: ignore
		CLEANED_DATA_DIR,
		STATISTICS_DATA_DIR,
		PROCESSED_DATA_DIR,
		CLEANED_BOOKS_FILENAME,
		PROCESSED_BOOKS_FILENAME,
		TEXT_NORMALIZE_COLUMNS,
		PRICE_COLUMN_NAME,
		TEXT_NORMALIZE_REPORT_FILENAME,
		PRICE_TRANSFORM_REPORT_FILENAME,
		COLUMN_TYPES_REPORT_FILENAME,
		ID_COLUMN_NAME,
		ID_REPORT_FILENAME,
		ID_DIGITS,
	)
except ModuleNotFoundError:  # Execução direta
	if str(PROJECT_ROOT) not in sys.path:
		sys.path.append(str(PROJECT_ROOT))
	from scripts.configs import (  # type: ignore
		CLEANED_DATA_DIR,
		STATISTICS_DATA_DIR,
		PROCESSED_DATA_DIR,
		CLEANED_BOOKS_FILENAME,
		PROCESSED_BOOKS_FILENAME,
		TEXT_NORMALIZE_COLUMNS,
		PRICE_COLUMN_NAME,
		TEXT_NORMALIZE_REPORT_FILENAME,
		PRICE_TRANSFORM_REPORT_FILENAME,
		COLUMN_TYPES_REPORT_FILENAME,
			ID_COLUMN_NAME,
			ID_REPORT_FILENAME,
			ID_DIGITS,
	)


class DataTransformer:
	"""Classe responsável por transformar dados limpos em formato processado.

	Etapas:
	  1. Ler cleaned_books.csv
	  2. Inferir tipos das colunas e salvar relatório
	  3. Normalizar colunas de texto (lowercase, underscore, remove especiais)
	  4. Transformar coluna de preço em float
	  5. Salvar dataset processado
	"""

	def __init__(self, cleaned_filename: str = CLEANED_BOOKS_FILENAME):
		self.cleaned_path: Path = CLEANED_DATA_DIR / cleaned_filename
		self.df: Optional[pl.DataFrame] = None
		self.types_report: Optional[pl.DataFrame] = None
		self.text_report: Optional[pl.DataFrame] = None
		self.price_report: Optional[pl.DataFrame] = None
		self.id_report: Optional[pl.DataFrame] = None

		STATISTICS_DATA_DIR.mkdir(parents=True, exist_ok=True)
		PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

	# 1. Carrega arquivo cleaned
	def read_cleaned(self) -> pl.DataFrame:
		if not self.cleaned_path.exists():
			raise FileNotFoundError(f"Arquivo cleaned não encontrado: {self.cleaned_path}")
		self.df = pl.read_csv(self.cleaned_path)
		return self.df

	# 2. Infere tipos de colunas
	def infer_column_types(self) -> pl.DataFrame:
		if self.df is None:
			raise ValueError("DataFrame não carregado. Chame read_cleaned().")

		rows: List[Dict[str, str]] = []
		for col, dtype in zip(self.df.columns, self.df.dtypes):
			kind = self._map_dtype(dtype)
			rows.append({
				"column": col,
				"polars_dtype": str(dtype),
				"inferred_kind": kind,
			})
		self.types_report = pl.DataFrame(rows)
		self.types_report.write_csv(STATISTICS_DATA_DIR / COLUMN_TYPES_REPORT_FILENAME)
		return self.types_report

	@staticmethod
	def _map_dtype(dtype: pl.DataType) -> str:
		if pl.datatypes.is_numeric(dtype):
			return "numeric"
		if dtype in (pl.Utf8, pl.Categorical):
			return "categorical"
		if dtype == pl.Boolean:
			return "boolean"
		return "other"

	# 3. Normaliza colunas de texto
	def normalize_text_columns(self) -> pl.DataFrame:
		if self.df is None:
			raise ValueError("DataFrame não carregado.")

		report: List[Dict[str, str]] = []
		df_proc = self.df
		pattern_non_alnum = re.compile(r"[^a-z0-9]+")

		for col in TEXT_NORMALIZE_COLUMNS:
			if col not in df_proc.columns:
				report.append({
					"column": col,
					"status": "missing",
					"changes": "column_not_present",
				})
				continue
			before_example = df_proc[col].head(1).to_list()[0] if df_proc[col].len() > 0 else ""
			df_proc = df_proc.with_columns(
				pl.col(col)
				.cast(pl.Utf8)
				.str.to_lowercase()
				.map_elements(lambda s: pattern_non_alnum.sub("_", s).strip("_"), return_dtype=pl.Utf8)
				.str.replace_all(r"__+", "_")
				.alias(col)
			)
			after_example = df_proc[col].head(1).to_list()[0] if df_proc[col].len() > 0 else ""
			report.append({
				"column": col,
				"status": "normalized",
				"changes": f"example_before='{before_example}' example_after='{after_example}'",
			})

		self.df = df_proc
		self.text_report = pl.DataFrame(report)
		self.text_report.write_csv(STATISTICS_DATA_DIR / TEXT_NORMALIZE_REPORT_FILENAME)
		return self.df

	# 4. Transforma coluna de preço em float
	def transform_price(self) -> pl.DataFrame:
		if self.df is None:
			raise ValueError("DataFrame não carregado.")
		if PRICE_COLUMN_NAME not in self.df.columns:
			self.price_report = pl.DataFrame([
				{"status": "missing_price_column", "details": f"coluna '{PRICE_COLUMN_NAME}' ausente"}
			])
			self.price_report.write_csv(STATISTICS_DATA_DIR / PRICE_TRANSFORM_REPORT_FILENAME)
			return self.df

		# Tentativa de limpeza: remover símbolos monetários, trocar vírgula por ponto, remover espaços
		def parse_price(val: str) -> float | None:
			if val is None:
				return None
			if not isinstance(val, str):
				try:
					return float(val)  # talvez já seja numérico
				except Exception:
					return None
			cleaned = val.strip()
			cleaned = re.sub(r"[\s]", "", cleaned)
			cleaned = re.sub(r"[^0-9,\.]", "", cleaned)  # remove letras/símbolos exceto , .
			# Se houver ambas , e . tentar heurística: se vírgula aparece depois do ponto, assume vírgula decimal
			if "," in cleaned and "." in cleaned:
				# Remove separador de milhar supondo que seja ponto
				# Ex: 1.234,56 -> 1234,56
				if cleaned.find(",") > cleaned.find("."):
					cleaned = cleaned.replace(".", "")
				cleaned = cleaned.replace(",", ".")
			else:
				# Apenas vírgula -> decimal
				if "," in cleaned and "." not in cleaned:
					cleaned = cleaned.replace(",", ".")
			try:
				return float(cleaned) if cleaned else None
			except Exception:
				return None

		df_proc = self.df.with_columns(
			pl.col(PRICE_COLUMN_NAME)
			.cast(pl.Utf8)
			.map_elements(parse_price, return_dtype=pl.Float64)
			.alias(PRICE_COLUMN_NAME)
		)
		after_null = df_proc[PRICE_COLUMN_NAME].null_count()
		total = df_proc.height
		converted_non_null = total - after_null

		self.df = df_proc
		self.price_report = pl.DataFrame([
			{
				"column": PRICE_COLUMN_NAME,
				"rows": total,
				"nulls_after": after_null,
				"converted_values": converted_non_null,
				"pct_converted": f"{(converted_non_null / total):.2%}" if total else "0%",
			}
		])
		self.price_report.write_csv(STATISTICS_DATA_DIR / PRICE_TRANSFORM_REPORT_FILENAME)
		return self.df

	# 4b. Adiciona coluna de ID único aleatório de N dígitos
	def add_unique_id(self) -> pl.DataFrame:
		if self.df is None:
			raise ValueError("DataFrame não carregado.")
		if ID_COLUMN_NAME in self.df.columns:
			# Não sobrescreve; registra e retorna
			self.id_report = pl.DataFrame([
				{"status": "id_column_exists", "rows": self.df.height, "unique_ids": self.df.select(pl.col(ID_COLUMN_NAME).n_unique()).item()}
			])
			self.id_report.write_csv(STATISTICS_DATA_DIR / ID_REPORT_FILENAME)
			return self.df

		import random
		random.seed()
		rows = self.df.height
		capacity = 9 * (10 ** (ID_DIGITS - 1))  # para 4 dígitos: 9000 (1000-9999)
		if rows > capacity:
			raise ValueError(f"Número de linhas ({rows}) excede capacidade de IDs únicos de {capacity} para {ID_DIGITS} dígitos.")

		low = 10 ** (ID_DIGITS - 1)
		high = (10 ** ID_DIGITS) - 1
		population = list(range(low, high + 1))
		ids = random.sample(population, rows)

		self.df = self.df.with_columns(pl.Series(ID_COLUMN_NAME, ids))
		self.id_report = pl.DataFrame([
			{
				"status": "generated",
				"rows": rows,
				"digits": ID_DIGITS,
				"min": min(ids) if ids else None,
				"max": max(ids) if ids else None,
				"unique_ids": len(set(ids)),
			}
		])
		self.id_report.write_csv(STATISTICS_DATA_DIR / ID_REPORT_FILENAME)
		return self.df

	# 5. Salva arquivo processado
	def save_processed(self, filename: str = PROCESSED_BOOKS_FILENAME) -> Path:
		if self.df is None:
			raise ValueError("DataFrame não carregado.")
		out_path = PROCESSED_DATA_DIR / filename
		self.df.write_csv(out_path)
		return out_path

	def run(self) -> Path:
		self.read_cleaned()
		self.infer_column_types()
		self.add_unique_id()
		self.normalize_text_columns()
		self.transform_price()
		return self.save_processed()


if __name__ == "__main__":
	transformer = DataTransformer()
	processed_path = transformer.run()
	print(f"Dataset processado salvo em: {processed_path}")
