import sys
from pathlib import Path
import tempfile
import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from scripts import pipeline_runner, cleaning, transformation, feature_engineering, exploratory_data_analysis  # type: ignore

@pytest.fixture()
def env_pipeline():
    tmpdir_obj = tempfile.TemporaryDirectory()
    base = Path(tmpdir_obj.name)
    # Build minimal data/raw
    raw_dir = base / 'data' / 'raw'
    raw_dir.mkdir(parents=True, exist_ok=True)
    stats_dir = base / 'data' / 'statistics'
    stats_dir.mkdir(parents=True, exist_ok=True)
    cleaned_dir = base / 'data' / 'cleaned'
    processed_dir = base / 'data' / 'processed'
    features_dir = base / 'data' / 'features'
    for d in [cleaned_dir, processed_dir, features_dir]:
        d.mkdir(exist_ok=True)
    # Monkeypatch configuration modules (override global paths used inside modules)
    cleaning.RAW_BOOKS_FILE = raw_dir / 'raw_books.csv'
    # Sample raw data
    pl.DataFrame([
        {"title": "Book A", "price": 10, "rating": 5, "availability": "In", "category": "c1", "image": "imga"},
        {"title": "Book B", "price": 12, "rating": 4, "availability": "In", "category": "c2", "image": "imgb"},
    ]).write_csv(cleaning.RAW_BOOKS_FILE)
    # Redirect directories
    for mod in [cleaning, transformation, feature_engineering, exploratory_data_analysis]:
        mod.STATISTICS_DATA_DIR = stats_dir
    cleaning.CLEANED_DIR = cleaned_dir
    transformation.CLEANED_DATA_DIR = cleaned_dir
    transformation.PROCESSED_DATA_DIR = processed_dir
    feature_engineering.PROCESSED_DATA_DIR = processed_dir
    feature_engineering.FEATURES_DATA_DIR = features_dir
    exploratory_data_analysis.CLEANED_DATA_DIR = cleaned_dir
    exploratory_data_analysis.PROCESSED_DATA_DIR = processed_dir
    exploratory_data_analysis.FEATURES_DATA_DIR = features_dir
    yield {"base": base}
    tmpdir_obj.cleanup()


def test_full_pipeline(env_pipeline):
    runner = pipeline_runner.PipelineRunner()
    report_path = runner.run()
    assert report_path.exists()
    # Basic integrity: all steps attempted
    step_names = [r.name for r in runner.results]
    for expected in ["data_cleaning", "data_transformation", "feature_engineering", "eda"]:
        assert expected in step_names
    # Report should have success statuses
    statuses = {r.name: r.status for r in runner.results}
    assert statuses.get("data_cleaning") == "success"
