from __future__ import annotations
import sys
from pathlib import Path
from typing import Optional, List, Dict
import polars as pl

# Utilitários comuns
from scripts.utils import (
	DataFrameValidator,
	DirectoryManager,
	DataTypeHelper,
	TextProcessor,
	NumericProcessor,
	IdGenerator
)

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
		ID_DIGITS
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
		ID_DIGITS
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

		DirectoryManager.ensure_directories(STATISTICS_DATA_DIR, PROCESSED_DATA_DIR)

	# 1. Carrega arquivo cleaned
	def read_cleaned(self) -> pl.DataFrame:
		"""Lê o dataset limpo do disco para self.df."""
		DirectoryManager.validate_file_exists(self.cleaned_path, "Arquivo cleaned")
		self.df = pl.read_csv(self.cleaned_path)
		return self.df

	# 2. Infere tipos de colunas
	def infer_column_types(self) -> pl.DataFrame:
		"""Infere e registra tipos/kinds das colunas atuais."""
		DataFrameValidator.validate_not_none(self.df, "DataFrame")

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
		"""Mapeia dtype Polars para categoria semântica simples."""
		return DataTypeHelper.map_dtype_to_category(dtype)


	# 3. Normaliza colunas de texto
	def normalize_text_columns(self) -> pl.DataFrame:
		"""Normaliza texto (lowercase, underscores, remove símbolos) nas colunas configuradas."""
		DataFrameValidator.validate_not_none(self.df, "DataFrame")

		report: List[Dict[str, str]] = []
		df_proc = self.df

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
				.map_elements(TextProcessor.normalize_text, return_dtype=pl.Utf8)
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
		"""Converte coluna de preço para float limpando formatos variados."""
		DataFrameValidator.validate_not_none(self.df, "DataFrame")
		if PRICE_COLUMN_NAME not in self.df.columns:
			self.price_report = pl.DataFrame([
				{"status": "missing_price_column", "details": f"coluna '{PRICE_COLUMN_NAME}' ausente"}
			])
			self.price_report.write_csv(STATISTICS_DATA_DIR / PRICE_TRANSFORM_REPORT_FILENAME)
			return self.df

		# Usa NumericProcessor para parsing de preço
		df_proc = self.df.with_columns(
			pl.col(PRICE_COLUMN_NAME)
			.cast(pl.Utf8)
			.map_elements(NumericProcessor.parse_price, return_dtype=pl.Float64)
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
		"""Gera IDs únicos pseudoaleatórios de N dígitos se coluna ainda não existir."""
		DataFrameValidator.validate_not_none(self.df, "DataFrame")
		if ID_COLUMN_NAME in self.df.columns:
			# Não sobrescreve; registra e retorna
			self.id_report = pl.DataFrame([
				{"status": "id_column_exists", "rows": self.df.height, "unique_ids": self.df.select(pl.col(ID_COLUMN_NAME).n_unique()).item()}
			])
			self.id_report.write_csv(STATISTICS_DATA_DIR / ID_REPORT_FILENAME)
			return self.df

		rows = self.df.height
		ids = IdGenerator.generate_unique_ids(rows, ID_DIGITS)

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
		"""Salva DataFrame processado em CSV e retorna caminho."""
		DataFrameValidator.validate_not_none(self.df, "DataFrame")
		out_path = PROCESSED_DATA_DIR / filename
		self.df.write_csv(out_path)
		return out_path

	def run(self) -> Path:
		"""Executa pipeline: read -> tipos -> id -> texto -> preço -> salva."""
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
