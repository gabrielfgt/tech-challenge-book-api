import sys
from pathlib import Path
import tempfile
import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from scripts import transformation  # type: ignore

@pytest.fixture()
def env_transformation():
    tmpdir_obj = tempfile.TemporaryDirectory()
    base = Path(tmpdir_obj.name)
    cleaned = base / 'data' / 'cleaned'
    stats = base / 'data' / 'statistics'
    processed = base / 'data' / 'processed'
    for d in [cleaned, stats, processed]:
        d.mkdir(parents=True, exist_ok=True)
    # Monkeypatch
    transformation.CLEANED_DATA_DIR = cleaned
    transformation.STATISTICS_DATA_DIR = stats
    transformation.PROCESSED_DATA_DIR = processed
    transformation.CLEANED_BOOKS_FILENAME = 'cleaned_books.csv'
    transformation.PROCESSED_BOOKS_FILENAME = 'processed_books.csv'
    transformation.TEXT_NORMALIZE_COLUMNS = ['title', 'category']
    transformation.PRICE_COLUMN_NAME = 'price'
    transformation.ID_COLUMN_NAME = 'id'
    # Sample data
    pl.DataFrame([
        {"title": "Some Book", "category": "Sci Fi", "price": "R$ 10,99"},
        {"title": "Another+Book", "category": "Drama & Suspense", "price": "12.50"},
    ]).write_csv(cleaned / transformation.CLEANED_BOOKS_FILENAME)
    yield {"base": base, "processed": processed}
    tmpdir_obj.cleanup()


def test_full_transformation(env_transformation):
    transformer = transformation.DataTransformer()
    out_path = transformer.run()
    assert out_path.exists()
    df_out = pl.read_csv(out_path)
    assert 'id' in df_out.columns
    assert 'price' in df_out.columns
    # Titles normalized
    assert all(isinstance(v, str) for v in df_out['title'].to_list())
    # Price is numeric
    assert df_out['price'].dtype in [pl.Float64, pl.Float32]
