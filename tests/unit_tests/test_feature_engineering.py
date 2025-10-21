import sys
from pathlib import Path
import tempfile
import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from scripts import feature_engineering  # type: ignore

@pytest.fixture()
def env_fe():
    tmpdir_obj = tempfile.TemporaryDirectory()
    base = Path(tmpdir_obj.name)
    processed = base / 'data' / 'processed'
    stats = base / 'data' / 'statistics'
    features = base / 'data' / 'features'
    for d in [processed, stats, features]:
        d.mkdir(parents=True, exist_ok=True)
    # Monkeypatch
    feature_engineering.PROCESSED_DATA_DIR = processed
    feature_engineering.STATISTICS_DATA_DIR = stats
    feature_engineering.FEATURES_DATA_DIR = features
    feature_engineering.PROCESSED_BOOKS_FILENAME = 'processed_books.csv'
    feature_engineering.TRAIN_FEATURES_FILENAME = 'train_features.csv'
    feature_engineering.TEST_FEATURES_FILENAME = 'test_features.csv'
    feature_engineering.FEATURES_FULL_FILENAME = 'features_full.csv'
    feature_engineering.FEATURE_PRICE_MINMAX_COLUMN = 'price'
    feature_engineering.PRICE_COLUMN_NAME = 'price'
    feature_engineering.OUTLIER_COLUMNS = ['price']
    feature_engineering.OUTLIER_IQR_FACTOR = 1.5
    feature_engineering.FEATURE_SELECTION_TARGET = 'price'
    pl.DataFrame({
        'price': [10, 11, 12, 13, 14, 1000],
        'title_len': [5,6,7,8,9,10],
        'category': ['a','b','a','b','a','b']
    }).write_csv(processed / feature_engineering.PROCESSED_BOOKS_FILENAME)
    yield {"base": base}
    tmpdir_obj.cleanup()


def test_feature_engineer_pipeline(env_fe):
    fe = feature_engineering.FeatureEngineer()
    outputs = fe.run()
    assert 'train' in outputs and 'test' in outputs
    train_df = pl.read_csv(outputs['train'])
    assert train_df['price'].max() < 1000
    assert any(c.endswith('_minmax') for c in train_df.columns)
    assert feature_engineering.FEATURE_SELECTION_TARGET in train_df.columns
