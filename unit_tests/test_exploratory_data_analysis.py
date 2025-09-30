import sys
from pathlib import Path
import tempfile
import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from scripts import exploratory_data_analysis  # type: ignore

@pytest.fixture()
def env_eda():
    tmpdir_obj = tempfile.TemporaryDirectory()
    base = Path(tmpdir_obj.name)
    cleaned_dir = base / 'data' / 'cleaned'
    processed_dir = base / 'data' / 'processed'
    features_dir = base / 'data' / 'features'
    stats_dir = base / 'data' / 'statistics'
    for d in [cleaned_dir, processed_dir, features_dir, stats_dir]:
        d.mkdir(parents=True, exist_ok=True)
    # Monkeypatch config paths
    exploratory_data_analysis.CLEANED_DATA_DIR = cleaned_dir
    exploratory_data_analysis.PROCESSED_DATA_DIR = processed_dir
    exploratory_data_analysis.FEATURES_DATA_DIR = features_dir
    exploratory_data_analysis.STATISTICS_DATA_DIR = stats_dir
    exploratory_data_analysis.CLEANED_BOOKS_FILENAME = 'cleaned_books.csv'
    exploratory_data_analysis.PROCESSED_BOOKS_FILENAME = 'processed_books.csv'
    exploratory_data_analysis.FEATURES_FULL_FILENAME = 'features_full.csv'
    exploratory_data_analysis.EDA_CLEANED_PROFILE_FILENAME = 'eda_cleaned_profile.csv'
    exploratory_data_analysis.EDA_PROCESSED_PROFILE_FILENAME = 'eda_processed_profile.csv'
    exploratory_data_analysis.EDA_FEATURES_PROFILE_FILENAME = 'eda_features_profile.csv'
    exploratory_data_analysis.EDA_CORRELATION_REPORT_FILENAME = 'eda_correlations.csv'
    exploratory_data_analysis.FEATURE_SELECTION_TARGET = 'price'
    # Create small datasets
    pl.DataFrame({'price': [10, 11, 12], 'category': ['a','b','a']}).write_csv(cleaned_dir / 'cleaned_books.csv')
    pl.DataFrame({'price': [10, 11, 12, 13], 'category': ['a','b','a','b'], 'x': [1,2,3,4]}).write_csv(processed_dir / 'processed_books.csv')
    pl.DataFrame({'price': [10, 11, 12, 13], 'x': [1,2,3,4], 'y': [2,4,6,8]}).write_csv(features_dir / 'features_full.csv')
    yield {"base": base}
    tmpdir_obj.cleanup()


def test_eda_run(env_eda):
    eda = exploratory_data_analysis.ExploratoryDataAnalysis()
    outputs = eda.run()
    # Expect profiles keys present
    assert 'cleaned_profile' in outputs
    assert 'processed_profile' in outputs
    assert 'features_profile' in outputs
    assert 'features_correlations' in outputs
    # Correlations file should exist
    assert Path(outputs['features_correlations']).exists()
