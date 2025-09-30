import sys
from pathlib import Path
import tempfile
import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from scripts import cleaning  # type: ignore


@pytest.fixture()
def temp_env():
    tmpdir_obj = tempfile.TemporaryDirectory()
    base = Path(tmpdir_obj.name)
    raw_dir = base / 'data' / 'raw'
    cleaned_dir = base / 'data' / 'cleaned'
    stats_dir = base / 'data' / 'statistics'
    for d in [raw_dir, cleaned_dir, stats_dir]:
        d.mkdir(parents=True, exist_ok=True)
    # Monkeypatch module globals
    cleaning.RAW_BOOKS_FILE = raw_dir / 'raw_books.csv'
    cleaning.STATISTICS_DIR = stats_dir
    cleaning.CLEANED_DIR = cleaned_dir
    yield {
        'raw': raw_dir,
        'cleaned': cleaned_dir,
        'stats': stats_dir,
    }
    tmpdir_obj.cleanup()


def test_run_cleaning_pipeline(temp_env):
    data = [
        {"title": "Book A", "price": 10.0, "rating": 4, "availability": "In", "category": "cat_a", "image": "img1"},
        {"title": "Book A", "price": 10.0, "rating": 4, "availability": "In", "category": "cat_a", "image": "img1"},
        {"title": "Book B", "price": None, "rating": 5, "availability": "Out", "category": "cat_b", "image": "img2"},
    ]
    pl.DataFrame(data).write_csv(cleaning.RAW_BOOKS_FILE)
    cleaner = cleaning.DataCleaner(raw_file=cleaning.RAW_BOOKS_FILE)
    out_path = cleaner.run()
    assert out_path.exists(), "Cleaned file not created"
    df = pl.read_csv(out_path)
    assert df.height == 2, "Duplicate row not removed"
    assert 'price' in df.columns
    assert (cleaning.STATISTICS_DIR / cleaning.NULLS_REPORT_FILENAME).exists()
    assert (cleaning.STATISTICS_DIR / cleaning.DUPLICATES_REPORT_FILENAME).exists()
