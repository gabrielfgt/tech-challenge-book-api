from __future__ import annotations
import sys
from pathlib import Path
from typing import Optional, List, Dict, Tuple
import polars as pl
import math
import random
import numpy as np
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder
from sklearn.feature_selection import VarianceThreshold, SelectKBest, f_regression

# Ajuste path para execução direta
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
try:
	from scripts.configs import (  # type: ignore
		PROCESSED_DATA_DIR,
		FEATURES_DATA_DIR,
		STATISTICS_DATA_DIR,
		PROCESSED_BOOKS_FILENAME,
		FEATURES_FULL_FILENAME,
		TRAIN_FEATURES_FILENAME,
		TEST_FEATURES_FILENAME,
		FEATURE_ENGINEERING_REPORT_SCALING,
		CATEGORICAL_ENCODING_REPORT_FILENAME,
		EXTRA_FEATURES_REPORT_FILENAME,
		SPLIT_REPORT_FILENAME,
		FEATURE_PRICE_MINMAX_COLUMN,
		FEATURE_TEST_SIZE,
		FEATURE_RANDOM_SEED,
		MAX_CATEGORICAL_CARDINALITY_OHE,
		PRICE_COLUMN_NAME,
		ID_COLUMN_NAME,
		TEXT_FEATURES_REPORT_FILENAME,
		TEXT_TITLE_COLUMN,
		TEXT_CATEGORY_COLUMN,
		FEATURE_SELECTION_VARIANCE_THRESHOLD,
		FEATURE_SELECTION_K,
		FEATURE_SELECTION_REPORT_FILENAME,
		FEATURE_SELECTION_TARGET,
		OUTLIER_COLUMNS,
		OUTLIER_IQR_FACTOR,
		OUTLIER_REPORT_FILENAME,
		NUMERIC_DTYPES
	)
except ModuleNotFoundError:
	if str(PROJECT_ROOT) not in sys.path:
		sys.path.append(str(PROJECT_ROOT))
		from scripts.configs import (  # type: ignore
			PROCESSED_DATA_DIR,
			FEATURES_DATA_DIR,
			STATISTICS_DATA_DIR,
			PROCESSED_BOOKS_FILENAME,
			FEATURES_FULL_FILENAME,
			TRAIN_FEATURES_FILENAME,
			TEST_FEATURES_FILENAME,
			FEATURE_ENGINEERING_REPORT_SCALING,
			CATEGORICAL_ENCODING_REPORT_FILENAME,
			EXTRA_FEATURES_REPORT_FILENAME,
			SPLIT_REPORT_FILENAME,
			FEATURE_PRICE_MINMAX_COLUMN,
			FEATURE_TEST_SIZE,
			FEATURE_RANDOM_SEED,
			MAX_CATEGORICAL_CARDINALITY_OHE,
			PRICE_COLUMN_NAME,
			ID_COLUMN_NAME,
			TEXT_FEATURES_REPORT_FILENAME,
			TEXT_TITLE_COLUMN,
			TEXT_CATEGORY_COLUMN,
			FEATURE_SELECTION_VARIANCE_THRESHOLD,
			FEATURE_SELECTION_K,
			FEATURE_SELECTION_REPORT_FILENAME,
			FEATURE_SELECTION_TARGET,
			OUTLIER_COLUMNS,
			OUTLIER_IQR_FACTOR,
			OUTLIER_REPORT_FILENAME,
		)


class FeatureEngineer:
	"""Classe para geração e transformação de features.

	Etapas principais (ordem de execução):
	  1. Leitura do dataset processado
	  2. Split treino/teste
	  3. Remoção de outliers (train define limites; aplica em test)
	  4. Features de texto
	  5. Escalonamento min-max do preço (fit em train)
	  6. One-hot encoding de categóricas (fit em train)
	  7. Criação de features adicionais (log, buckets)
	  8. Seleção de features (VarianceThreshold + SelectKBest)
	  9. Salvamento de train/test/full + relatórios
	"""

	def __init__(self, processed_filename: str = PROCESSED_BOOKS_FILENAME):
		self.processed_path = PROCESSED_DATA_DIR / processed_filename
		self.df: Optional[pl.DataFrame] = None
		self.train_df: Optional[pl.DataFrame] = None
		self.test_df: Optional[pl.DataFrame] = None
		self.features_train: Optional[pl.DataFrame] = None
		self.features_test: Optional[pl.DataFrame] = None
		self.scaling_report: Optional[pl.DataFrame] = None
		self.categorical_report: Optional[pl.DataFrame] = None
		self.extra_report: Optional[pl.DataFrame] = None
		self.split_report: Optional[pl.DataFrame] = None
		self.text_report: Optional[pl.DataFrame] = None
		self.feature_selection_report: Optional[pl.DataFrame] = None
		self.outlier_report: Optional[pl.DataFrame] = None

		FEATURES_DATA_DIR.mkdir(parents=True, exist_ok=True)
		STATISTICS_DATA_DIR.mkdir(parents=True, exist_ok=True)

	# 1. Ler dataset processado
	def read_processed(self) -> pl.DataFrame:
		"""Lê o dataset processado do disco para self.df."""
		if not self.processed_path.exists():
			raise FileNotFoundError(f"Arquivo processado não encontrado: {self.processed_path}")
		self.df = pl.read_csv(self.processed_path)
		return self.df

	# 2. Split train/test
	def initial_split(self) -> None:
		"""Realiza split aleatório train/test preservando proporção configurada."""
		if self.df is None:
			raise ValueError("DataFrame não carregado.")
		df = self.df
		total = df.height
		if total == 0:
			raise ValueError("Dataset vazio para split.")
		test_size = int(math.ceil(total * FEATURE_TEST_SIZE))
		random.seed(FEATURE_RANDOM_SEED)
		indices = list(range(total))
		random.shuffle(indices)
		test_indices = set(indices[:test_size])
		train_rows = []
		test_rows = []
		for idx, row in enumerate(df.iter_rows()):
			(test_rows if idx in test_indices else train_rows).append(row)
		cols = df.columns
		self.train_df = pl.DataFrame(train_rows, schema=cols, orient="row")
		self.test_df = pl.DataFrame(test_rows, schema=cols, orient="row")
		self.split_report = pl.DataFrame([
			{
				"total_rows": total,
				"train_rows": self.train_df.height,
				"test_rows": self.test_df.height,
				"test_ratio_requested": FEATURE_TEST_SIZE,
				"test_ratio_observed": f"{(self.test_df.height/total):.2%}",
			}
		])
		self.split_report.write_csv(STATISTICS_DATA_DIR / SPLIT_REPORT_FILENAME)
		return None

	# 3. Remoção de outliers pós-split
	def remove_outliers(self) -> None:  
		"""Remove outliers das colunas configuradas com limites calculados no train e aplicados ao test."""
		if self.train_df is None:
			raise ValueError("Train não definido.")
		train = self.train_df
		test = self.test_df
		records: List[Dict[str,str]] = []
		for col in OUTLIER_COLUMNS:
			if col not in train.columns:
				records.append({"column": col, "status": "missing"})
				continue
			if train[col].dtype not in NUMERIC_DTYPES:
				records.append({"column": col, "status": f"non_numeric({train[col].dtype})"})
				continue
			series = train[col]
			q1 = series.quantile(0.25)
			q3 = series.quantile(0.75)
			if q1 is None or q3 is None:
				records.append({"column": col, "status": "no_quantiles"})
				continue
			iqr = q3 - q1
			if iqr == 0:
				records.append({"column": col, "status": "zero_iqr", "q1": f"{q1}", "q3": f"{q3}"})
				continue
			lower = q1 - OUTLIER_IQR_FACTOR * iqr
			upper = q3 + OUTLIER_IQR_FACTOR * iqr
			rows_before_train = train.height
			train = train.filter((pl.col(col) >= lower) & (pl.col(col) <= upper) | pl.col(col).is_null())
			rows_after_train = train.height
			removed_train = rows_before_train - rows_after_train
			removed_test = 0
			if test is not None and col in test.columns:
				rows_before_test = test.height
				test = test.filter((pl.col(col) >= lower) & (pl.col(col) <= upper) | pl.col(col).is_null())
				removed_test = rows_before_test - test.height
			records.append({
				"column": col,
				"status": "filtered",
				"q1": f"{q1}",
				"q3": f"{q3}",
				"lower": f"{lower}",
				"upper": f"{upper}",
				"removed_train": str(removed_train),
				"removed_test": str(removed_test),
			})
		self.train_df = train
		self.test_df = test
		self.outlier_report = pl.DataFrame(records)
		self.outlier_report.write_csv(STATISTICS_DATA_DIR / OUTLIER_REPORT_FILENAME)
		return None

	# 4. Features de texto
	def add_text_features(self) -> None:  # 4. Text features
		"""Adiciona features de comprimento, contagem de palavras/tokens e flags numéricas em texto."""
		if self.train_df is None:
			raise ValueError("Train não definido.")
		train = self.train_df
		test = self.test_df
		records: List[Dict[str,str]] = []
		def safe_len(s):
			return len(s) if isinstance(s, str) else 0
		def word_count(s):
			return len(s.split()) if isinstance(s, str) else 0
		def has_number(s):
			return any(ch.isdigit() for ch in s) if isinstance(s, str) else False
		def cat_token_count(s):
			return len(s.split('_')) if isinstance(s, str) else 0
		if TEXT_TITLE_COLUMN in train.columns:
			train = train.with_columns([
				pl.col(TEXT_TITLE_COLUMN).cast(pl.Utf8).map_elements(safe_len, return_dtype=pl.Int64).alias(f"{TEXT_TITLE_COLUMN}_len"),
				pl.col(TEXT_TITLE_COLUMN).cast(pl.Utf8).map_elements(word_count, return_dtype=pl.Int64).alias(f"{TEXT_TITLE_COLUMN}_word_count"),
				pl.col(TEXT_TITLE_COLUMN).cast(pl.Utf8).map_elements(has_number, return_dtype=pl.Boolean).alias(f"{TEXT_TITLE_COLUMN}_has_number"),
			])
			if test is not None and TEXT_TITLE_COLUMN in test.columns:
				test = test.with_columns([
					pl.col(TEXT_TITLE_COLUMN).cast(pl.Utf8).map_elements(safe_len, return_dtype=pl.Int64).alias(f"{TEXT_TITLE_COLUMN}_len"),
					pl.col(TEXT_TITLE_COLUMN).cast(pl.Utf8).map_elements(word_count, return_dtype=pl.Int64).alias(f"{TEXT_TITLE_COLUMN}_word_count"),
					pl.col(TEXT_TITLE_COLUMN).cast(pl.Utf8).map_elements(has_number, return_dtype=pl.Boolean).alias(f"{TEXT_TITLE_COLUMN}_has_number"),
				])
			records.append({"group": TEXT_TITLE_COLUMN, "features": "len,word_count,has_number"})
		if TEXT_CATEGORY_COLUMN in train.columns:
			train = train.with_columns(
				pl.col(TEXT_CATEGORY_COLUMN).cast(pl.Utf8).map_elements(cat_token_count, return_dtype=pl.Int64).alias(f"{TEXT_CATEGORY_COLUMN}_token_count")
			)
			if test is not None and TEXT_CATEGORY_COLUMN in test.columns:
				test = test.with_columns(
					pl.col(TEXT_CATEGORY_COLUMN).cast(pl.Utf8).map_elements(cat_token_count, return_dtype=pl.Int64).alias(f"{TEXT_CATEGORY_COLUMN}_token_count")
				)
			records.append({"group": TEXT_CATEGORY_COLUMN, "features": "token_count"})
		self.train_df = train
		self.test_df = test
		self.text_report = pl.DataFrame(records) if records else pl.DataFrame([{"status": "no_text_features"}])
		self.text_report.write_csv(STATISTICS_DATA_DIR / TEXT_FEATURES_REPORT_FILENAME)
		return None

	# 5. Min-max scaling do preço
	def scale_price(self) -> None:
		"""Aplica MinMaxScaler (scikit-learn) na coluna de preço, gerando coluna <price>_minmax."""
		if self.train_df is None:
			raise ValueError("Train DataFrame não definido.")
		if FEATURE_PRICE_MINMAX_COLUMN not in self.train_df.columns:
			self.scaling_report = pl.DataFrame([
				{"status": "missing_price_column", "column": FEATURE_PRICE_MINMAX_COLUMN}
			])
			self.scaling_report.write_csv(STATISTICS_DATA_DIR / FEATURE_ENGINEERING_REPORT_SCALING)
			self.features_train = self.train_df
			self.features_test = self.test_df
			return

		col = FEATURE_PRICE_MINMAX_COLUMN
		values_train = self.train_df.select(pl.col(col)).to_numpy()
		values_test = self.test_df.select(pl.col(col)).to_numpy() if self.test_df is not None else np.array([])
		# Trata casos degenerados (todos iguais)
		if np.nanstd(values_train) == 0:
			scaled_train = np.zeros_like(values_train, dtype=float)
			scaled_test = np.zeros_like(values_test, dtype=float)
		else:
			scaler = MinMaxScaler()
			scaled_train = scaler.fit_transform(values_train)
			scaled_test = scaler.transform(values_test) if values_test.size else values_test

		self.features_train = self.train_df.with_columns(
			pl.Series(f"{col}_minmax", scaled_train.flatten())
		)
		self.features_test = self.test_df.with_columns(
			pl.Series(f"{col}_minmax", scaled_test.flatten())
		) if self.test_df is not None and values_test.size else self.test_df
		min_val = float(np.nanmin(values_train)) if values_train.size else None
		max_val = float(np.nanmax(values_train)) if values_train.size else None
		self.scaling_report = pl.DataFrame([
			{
				"column": col,
				"min": min_val,
				"max": max_val,
				"scaled_feature": f"{col}_minmax",
				"method": "sklearn.MinMaxScaler",
			}
		])
		self.scaling_report.write_csv(STATISTICS_DATA_DIR / FEATURE_ENGINEERING_REPORT_SCALING)
		return None

	# 6. One-hot encoding de categóricas
	def encode_categoricals(self) -> None:
		"""Aplica OneHotEncoder (sklearn) em colunas categóricas elegíveis."""
		if self.features_train is None:
			raise ValueError("Features train não definidas.")

		train_df = self.features_train
		test_df = self.features_test
		candidate_cols: List[Tuple[str,int]] = []
		for c, dt in zip(train_df.columns, train_df.dtypes):
			if c in {ID_COLUMN_NAME, PRICE_COLUMN_NAME, f"{PRICE_COLUMN_NAME}_minmax"}:
				continue
			if dt in (pl.Utf8, pl.Categorical):
				cardinality = train_df.select(pl.col(c).n_unique()).item()
				if cardinality <= MAX_CATEGORICAL_CARDINALITY_OHE:
					candidate_cols.append((c, cardinality))

		if not candidate_cols:
			self.categorical_report = pl.DataFrame([{"status": "no_categorical_columns_encoded"}])
			self.categorical_report.write_csv(STATISTICS_DATA_DIR / CATEGORICAL_ENCODING_REPORT_FILENAME)
			return None

		# Evita dependência de pandas/pyarrow convertendo diretamente para matriz numpy
		encoder = OneHotEncoder(sparse_output=False, handle_unknown="ignore")
		train_cols_values = [train_df.select(pl.col(c).cast(pl.Utf8)).to_series().to_list() for c,_ in candidate_cols]
		# shape: (n_samples, n_features)
		import numpy as _np  # local import para evitar poluir namespace global
		cat_train = _np.column_stack(train_cols_values) if len(train_cols_values) > 1 else _np.array(train_cols_values[0]).reshape(-1,1)
		encoded_train = encoder.fit_transform(cat_train)
		if test_df is not None:
			test_cols_values = [test_df.select(pl.col(c).cast(pl.Utf8)).to_series().to_list() for c,_ in candidate_cols]
			cat_test = _np.column_stack(test_cols_values) if len(test_cols_values) > 1 else _np.array(test_cols_values[0]).reshape(-1,1)
			encoded_test = encoder.transform(cat_test)
		else:
			encoded_test = None
		encoded_cols: List[str] = []
		for base_col, cats in zip([c for c,_ in candidate_cols], encoder.categories_):
			encoded_cols.extend([f"{base_col}__{cat}" for cat in cats])

		train_encoded_pl = pl.DataFrame(encoded_train, schema=encoded_cols)
		self.features_train = pl.concat([train_df, train_encoded_pl], how="horizontal")
		if encoded_test is not None:
			test_encoded_pl = pl.DataFrame(encoded_test, schema=encoded_cols)
			self.features_test = pl.concat([test_df, test_encoded_pl], how="horizontal") if test_df is not None else None
		self.categorical_report = pl.DataFrame([
			{
				"column": col,
				"cardinality": str(card),
				"created_features": str(sum(c.startswith(f"{col}__") for c in encoded_cols)),
				"encoder": "sklearn.OneHotEncoder",
			}
			for col, card in candidate_cols
		])
		self.categorical_report.write_csv(STATISTICS_DATA_DIR / CATEGORICAL_ENCODING_REPORT_FILENAME)
		return None

	# 7. Extra feature engineering (exemplos simples)
	def create_extra_features(self) -> None:
		"""Cria features derivadas simples (log do preço e buckets por quantis)."""
		if self.features_train is None:
			raise ValueError("Features train não definidas.")
		train_df = self.features_train
		test_df = self.features_test
		reports: List[Dict[str, str]] = []
		# Função auxiliar para aplicar transformações paralelas em train/test
		def apply_both(fn):
			return fn(train_df), (fn(test_df) if test_df is not None else None)

		if PRICE_COLUMN_NAME in train_df.columns:
			train_df = train_df.with_columns(
				pl.when(pl.col(PRICE_COLUMN_NAME) > 0)
				.then(pl.col(PRICE_COLUMN_NAME).log())
				.otherwise(None)
				.alias(f"{PRICE_COLUMN_NAME}_log")
			)
			if test_df is not None and PRICE_COLUMN_NAME in test_df.columns:
				test_df = test_df.with_columns(
					pl.when(pl.col(PRICE_COLUMN_NAME) > 0)
					.then(pl.col(PRICE_COLUMN_NAME).log())
					.otherwise(None)
					.alias(f"{PRICE_COLUMN_NAME}_log")
				)
			reports.append({"feature": f"{PRICE_COLUMN_NAME}_log", "source": PRICE_COLUMN_NAME, "type": "log_transform"})

		if PRICE_COLUMN_NAME in train_df.columns:
			try:
				quantiles = train_df.select(
					pl.col(PRICE_COLUMN_NAME).quantile([0.2, 0.4, 0.6, 0.8], interpolation="nearest")
				)[0].to_list()
				q20, q40, q60, q80 = quantiles
				train_df = train_df.with_columns(
					pl.when(pl.col(PRICE_COLUMN_NAME) <= q20).then(pl.lit(0))
					.when(pl.col(PRICE_COLUMN_NAME) <= q40).then(pl.lit(1))
					.when(pl.col(PRICE_COLUMN_NAME) <= q60).then(pl.lit(2))
					.when(pl.col(PRICE_COLUMN_NAME) <= q80).then(pl.lit(3))
					.otherwise(pl.lit(4))
					.alias(f"{PRICE_COLUMN_NAME}_bucket")
				)
				if test_df is not None and PRICE_COLUMN_NAME in test_df.columns:
					test_df = test_df.with_columns(
						pl.when(pl.col(PRICE_COLUMN_NAME) <= q20).then(pl.lit(0))
						.when(pl.col(PRICE_COLUMN_NAME) <= q40).then(pl.lit(1))
						.when(pl.col(PRICE_COLUMN_NAME) <= q60).then(pl.lit(2))
						.when(pl.col(PRICE_COLUMN_NAME) <= q80).then(pl.lit(3))
						.otherwise(pl.lit(4))
						.alias(f"{PRICE_COLUMN_NAME}_bucket")
					)
				reports.append({
					"feature": f"{PRICE_COLUMN_NAME}_bucket",
					"type": "quantile_bucket",
					"quantiles": str(quantiles),
				})
			except Exception as e:
				reports.append({"feature": f"{PRICE_COLUMN_NAME}_bucket", "status": f"failed: {e}"})

		self.features_train = train_df
		self.features_test = test_df
		self.extra_report = pl.DataFrame(reports) if reports else pl.DataFrame([
			{"status": "no_extra_features"}
		])
		self.extra_report.write_csv(STATISTICS_DATA_DIR / EXTRA_FEATURES_REPORT_FILENAME)
		return None

	# 8. Seleção de features
	def feature_selection(self) -> None:  
		"""Seleciona features numéricas via VarianceThreshold e SelectKBest (f_regression)."""
		if self.features_train is None:
			raise ValueError("Features train não definidas.")
		train = self.features_train
		test = self.features_test
		# Remove columns não numéricas antes de seleção
		numeric_cols = [c for c, dt in zip(train.columns, train.dtypes) if dt in (pl.Int64, pl.Int32, pl.Float64, pl.Float32)]
		if FEATURE_SELECTION_TARGET not in numeric_cols:
			self.feature_selection_report = pl.DataFrame([
				{"status": "target_not_numeric_or_missing", "target": FEATURE_SELECTION_TARGET}
			])
			self.feature_selection_report.write_csv(STATISTICS_DATA_DIR / FEATURE_SELECTION_REPORT_FILENAME)
			return None
		feature_cols = [c for c in numeric_cols if c != FEATURE_SELECTION_TARGET]
		X_train = train.select(feature_cols).to_numpy()
		y_train = train.select(pl.col(FEATURE_SELECTION_TARGET)).to_numpy().ravel()
		# Variance Threshold
		if FEATURE_SELECTION_VARIANCE_THRESHOLD > 0:
			vt = VarianceThreshold(threshold=FEATURE_SELECTION_VARIANCE_THRESHOLD)
			X_train = vt.fit_transform(X_train)
			feature_cols = [feature_cols[i] for i in vt.get_support(indices=True)]
		# SelectKBest
		k = min(FEATURE_SELECTION_K, len(feature_cols))
		selector = SelectKBest(score_func=f_regression, k=k)
		X_train = selector.fit_transform(X_train, y_train)
		selected = [feature_cols[i] for i in selector.get_support(indices=True)]
		# Reduz train/test às colunas selecionadas + target + id se houver
		cols_to_keep = selected + [FEATURE_SELECTION_TARGET]
		# Garante que não haja duplicata do ID
		if ID_COLUMN_NAME in train.columns and ID_COLUMN_NAME not in cols_to_keep:
			cols_to_keep.append(ID_COLUMN_NAME)
		self.features_train = train.select(cols_to_keep)
		if test is not None:
			available = [c for c in cols_to_keep if c in test.columns]
			self.features_test = test.select(available)
		self.feature_selection_report = pl.DataFrame([
			{"selected_feature": f, "rank": idx+1} for idx, f in enumerate(selected)
		])
		self.feature_selection_report.write_csv(STATISTICS_DATA_DIR / FEATURE_SELECTION_REPORT_FILENAME)
		return None

	# 9 (parcial). Salvar dataset completo de features (full); train/test salvos em run()
	def save_features(self) -> Dict[str, Path]:
		"""Salva dataset completo de features (train+test) e retorna caminhos."""
		if self.features_train is None:
			raise ValueError("Features train não geradas.")
		full_df = self.features_train
		if self.features_test is not None:
			full_df = pl.concat([self.features_train, self.features_test], how="vertical_relaxed")
		full_path = FEATURES_DATA_DIR / FEATURES_FULL_FILENAME
		full_df.write_csv(full_path)
		return {"full": full_path}

	def run(self) -> Dict[str, Path]:
		"""Executa pipeline: read -> split -> outliers -> texto -> escala -> OHE -> extras -> seleção -> salva."""
		self.read_processed()
		self.initial_split()
		self.remove_outliers()
		self.add_text_features()
		# Escala preço (usa apenas train para fit)
		self.scale_price()
		# Encoding categórico (fit em train, transform em test)
		self.encode_categoricals()
		# Extra features
		self.create_extra_features()
		# Seleção de features após criação
		self.feature_selection()
		# Salvar train/test e full
		paths = {}
		if self.features_train is not None:
			train_path = FEATURES_DATA_DIR / TRAIN_FEATURES_FILENAME
			self.features_train.write_csv(train_path)
			paths["train"] = train_path
		if self.features_test is not None:
			test_path = FEATURES_DATA_DIR / TEST_FEATURES_FILENAME
			self.features_test.write_csv(test_path)
			paths["test"] = test_path
		paths.update(self.save_features())
		return paths


if __name__ == "__main__":
	fe = FeatureEngineer()
	paths = fe.run()
	print(f"Features geradas. Train: {paths['train']} Test: {paths['test']}")
