import tempfile
from pathlib import Path

from extraction.parquet_writer import write_bronze_parquet


def test_write_bronze_parquet_crea_particiones():
    records = [
        {"id": 1, "content_type": "article", "title": "Test", "authors": []},
        {"id": 2, "content_type": "blog", "title": "Blog", "authors": []},
    ]
    with tempfile.TemporaryDirectory() as tmp:
        written = write_bronze_parquet(records, tmp)
        assert written == 2
        base = Path(tmp)
        assert (base / "content_type=article").exists() or any(base.rglob("*.parquet"))
        assert (base / "content_type=blog").exists() or any(base.rglob("*.parquet"))


def test_write_bronze_parquet_lista_vacia_no_escribe():
    with tempfile.TemporaryDirectory() as tmp:
        written = write_bronze_parquet([], tmp)
        assert written == 0
        assert not list(Path(tmp).rglob("*.parquet"))
