"""Exploratory Data Analysis utilities.

Gera perfis (estatísticas descritivas) para datasets cleaned, processed e features,
além de correlações com o target configurado. Resultados são salvos em CSV
no diretório de statistics.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import polars as pl
import numpy as np

# Utilitários comuns
from scripts.utils import (
	NumericProcessor,
	ReportGenerator,
	NUMERIC_DTYPES,
	STRING_DTYPES
)

try:  # imports locais via execução direta
	from scripts.configs import (  # type: ignore
		CLEANED_DATA_DIR,
		PROCESSED_DATA_DIR,
		FEATURES_DATA_DIR,
		STATISTICS_DATA_DIR,
		CLEANED_BOOKS_FILENAME,
		PROCESSED_BOOKS_FILENAME,
		FEATURES_FULL_FILENAME,
		EDA_CLEANED_PROFILE_FILENAME,
		EDA_PROCESSED_PROFILE_FILENAME,
		EDA_FEATURES_PROFILE_FILENAME,
		EDA_CORRELATION_REPORT_FILENAME,
		FEATURE_SELECTION_TARGET,
	)
except Exception:  # pragma: no cover
	from configs import *  # type: ignore  # noqa





@dataclass
class DatasetProfile:
	name: str
	dataframe: pl.DataFrame


class ExploratoryDataAnalysis:
	def __init__(self) -> None:
		"""Inicializa caminhos e placeholders para DataFrames usados na EDA."""
		self.cleaned_path: Path = CLEANED_DATA_DIR / CLEANED_BOOKS_FILENAME
		self.processed_path: Path = PROCESSED_DATA_DIR / PROCESSED_BOOKS_FILENAME
		self.features_path: Path = FEATURES_DATA_DIR / FEATURES_FULL_FILENAME
		self.cleaned: pl.DataFrame | None = None
		self.processed: pl.DataFrame | None = None
		self.features: pl.DataFrame | None = None

	# ------------------------------ Carregamento ------------------------------
	def load(self) -> None:
		"""Carrega datasets cleaned, processed e features se existirem."""
		if self.cleaned_path.exists():
			self.cleaned = pl.read_csv(self.cleaned_path)
		if self.processed_path.exists():
			self.processed = pl.read_csv(self.processed_path)
		if self.features_path.exists():
			self.features = pl.read_csv(self.features_path)

	# ------------------------- Estatísticas numéricas -------------------------
	def numeric_stats(self, df: pl.DataFrame) -> pl.DataFrame:
		"""Calcula estatísticas descritivas para colunas numéricas (contagem, média, quantis, etc.)."""
		numeric_cols = [c for c, dt in zip(df.columns, df.dtypes) if dt in NUMERIC_DTYPES]
		if not numeric_cols:
			return pl.DataFrame([{"status": "no_numeric_columns"}])
		stats_rows: List[Dict[str, str | float | int]] = []
		for col in numeric_cols:
			s = df[col]
			non_null = s.len() - s.null_count()
			if non_null == 0:
				stats_rows.append({"column": col, "status": "all_null"})
				continue
			values = s.drop_nulls().to_numpy()
			zeros = int((values == 0).sum()) if values.size else 0
			uniq = int(np.unique(values).shape[0]) if values.size else 0
			quantiles = np.quantile(values, [0.25, 0.5, 0.75]) if values.size else [None, None, None]
			stats_rows.append({
				"column": col,
				"count": int(s.len()),
				"missing": int(s.null_count()),
				"mean": float(np.mean(values)) if values.size else None,
				"std": float(np.std(values, ddof=1)) if values.size > 1 else None,
				"min": float(np.min(values)) if values.size else None,
				"p25": float(quantiles[0]) if values.size else None,
				"p50": float(quantiles[1]) if values.size else None,
				"p75": float(quantiles[2]) if values.size else None,
				"max": float(np.max(values)) if values.size else None,
				"zeros": zeros,
				"unique": uniq,
			})
		return pl.DataFrame(stats_rows)

	# ------------------------ Estatísticas categóricas ------------------------
	def categorical_stats(self, df: pl.DataFrame) -> pl.DataFrame:
		"""Resumo de colunas categóricas: cardinalidade, valor mais frequente e missing."""
		cat_cols = [c for c, dt in zip(df.columns, df.dtypes) if dt in STRING_DTYPES]
		if not cat_cols:
			return pl.DataFrame([{"status": "no_categorical_columns"}])
		rows: List[Dict[str, str | int | float]] = []
		for col in cat_cols:
			s = df[col]
			total = s.len()
			missing = s.null_count()
			non_null_series = s.drop_nulls()
			if non_null_series.len() == 0:
				rows.append({"column": col, "status": "all_null"})
				continue
			vc = (
				non_null_series.to_frame().group_by(col).agg(pl.len().alias("freq")).sort("freq", descending=True)
			)
			top_row = vc.row(0)
			top_val, top_freq = top_row[0], top_row[1]
			rows.append({
				"column": col,
				"unique": int(vc.height),
				"top": str(top_val),
				"top_freq": int(top_freq),
				"top_pct": float(top_freq / (total - missing)) if (total - missing) > 0 else None,
				"missing": int(missing),
			})
		return pl.DataFrame(rows)

	# --------------------------- Correlações numéricas ------------------------
	def correlations(self, df: pl.DataFrame) -> pl.DataFrame:
		"""
		Computa correlações Pearson.
		Caso a coluna target configurada exista, retorna correlação de cada numérica com ela.
		Caso contrário, gera correlações pairwise entre todas as colunas numéricas.
		Se houver menos de duas colunas numéricas, retorna status e imprime aviso.
		"""
		numeric_cols_all = [c for c, dt in zip(df.columns, df.dtypes) if dt in NUMERIC_DTYPES]
		# Caminho 1: target presente -> manter comportamento anterior
		if FEATURE_SELECTION_TARGET in df.columns:
			other_numeric = [c for c in numeric_cols_all if c != FEATURE_SELECTION_TARGET]
			if not other_numeric:
				return pl.DataFrame([{"status": "no_numeric_columns_except_target"}])
			target = df[FEATURE_SELECTION_TARGET]
			if target.null_count() == target.len():
				return pl.DataFrame([{"status": "target_all_null"}])
			rows: List[Dict[str, str | float]] = []
			tgt_np = target.to_numpy()
			for col in other_numeric:
				col_np = df[col].to_numpy()
				corr = NumericProcessor.safe_correlation(col_np, tgt_np)
				rows.append({"feature": col, "correlation_with_target": corr})
			return pl.DataFrame(rows)
		# Caminho 2: sem target -> correlação pairwise
		if len(numeric_cols_all) < 2:
			print("[INFO] Correlações: menos de duas colunas numéricas disponíveis; pulando.")
			return pl.DataFrame([
				{"status": "insufficient_numeric_columns", "numeric_count": len(numeric_cols_all)}
			])
		rows: List[Dict[str, str | float]] = []
		for i in range(len(numeric_cols_all)):
			for j in range(i + 1, len(numeric_cols_all)):
				c1 = numeric_cols_all[i]
				c2 = numeric_cols_all[j]
				v1 = df[c1].to_numpy()
				v2 = df[c2].to_numpy()
				corr = NumericProcessor.safe_correlation(v1, v2)
				rows.append({"feature_a": c1, "feature_b": c2, "pearson_corr": corr})
		return pl.DataFrame(rows)

	# ------------------------------- Perfil geral -----------------------------
	def build_profile(self, name: str, df: pl.DataFrame) -> pl.DataFrame:
		"""Combina estatísticas numéricas e categóricas em um único DataFrame rotulado."""
		num_df = self.numeric_stats(df)
		cat_df = self.categorical_stats(df)
		# adicionar prefixos para distinguir
		num_df = num_df.with_columns([pl.lit("numeric").alias("type"), pl.lit(name).alias("dataset")])
		cat_df = cat_df.with_columns([pl.lit("categorical").alias("type"), pl.lit(name).alias("dataset")])
		# alinhar colunas
		all_cols = sorted(set(num_df.columns) | set(cat_df.columns))
		num_df = num_df.select([pl.col(c) if c in num_df.columns else pl.lit(None).alias(c) for c in all_cols])
		cat_df = cat_df.select([pl.col(c) if c in cat_df.columns else pl.lit(None).alias(c) for c in all_cols])
		# Usa vertical_relaxed para permitir coerção de tipos quando algumas colunas são None em um dos blocos
		return pl.concat([num_df, cat_df], how="vertical_relaxed")

	# ------------------------------ Execução total ----------------------------
	def run(self) -> Dict[str, Path]:
		"""Gera perfis para cada dataset disponível e correlações em features."""
		self.load()
		outputs: Dict[str, Path] = {}
		
		# Gerar perfis usando ReportGenerator
		if self.cleaned is not None:
			prof = self.build_profile("cleaned", self.cleaned)
			prof_data = prof.to_dicts()
			p = ReportGenerator.save_report(
				prof_data, 
				STATISTICS_DATA_DIR / EDA_CLEANED_PROFILE_FILENAME,
				"perfil_dataset_limpo"
			)
			outputs["cleaned_profile"] = p
			
		if self.processed is not None:
			prof = self.build_profile("processed", self.processed)
			prof_data = prof.to_dicts()
			p = ReportGenerator.save_report(
				prof_data,
				STATISTICS_DATA_DIR / EDA_PROCESSED_PROFILE_FILENAME,
				"perfil_dataset_processado"
			)
			outputs["processed_profile"] = p
			
		if self.features is not None:
			prof = self.build_profile("features", self.features)
			prof_data = prof.to_dicts()
			p = ReportGenerator.save_report(
				prof_data,
				STATISTICS_DATA_DIR / EDA_FEATURES_PROFILE_FILENAME,
				"perfil_dataset_features"
			)
			outputs["features_profile"] = p
			
			# correlações apenas no dataset de features (normalmente já pós seleção)
			corr_df = self.correlations(self.features)
			corr_data = corr_df.to_dicts()
			corr_path = ReportGenerator.save_report(
				corr_data,
				STATISTICS_DATA_DIR / EDA_CORRELATION_REPORT_FILENAME,
				"correlacoes_features"
			)
			outputs["features_correlations"] = corr_path
			
		return outputs


def main() -> None:  # simples execução scriptável
	eda = ExploratoryDataAnalysis()
	out = eda.run()
	print("EDA reports:")
	for k, v in out.items():
		print(f" - {k}: {v}")


if __name__ == "__main__":  # pragma: no cover
	main()

