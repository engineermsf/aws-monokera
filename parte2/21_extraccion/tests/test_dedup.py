from extraction.run import _dedup_by_content_type_id


def test_dedup_elimina_duplicados_mismo_content_type_id():
    records = [
        {"id": 1, "content_type": "article", "title": "A"},
        {"id": 1, "content_type": "article", "title": "B"},
        {"id": 2, "content_type": "article", "title": "C"},
    ]
    out = _dedup_by_content_type_id(records)
    assert len(out) == 2
    assert out[0]["title"] == "A"
    assert out[1]["title"] == "C"


def test_dedup_mantiene_mismo_id_distinto_content_type():
    records = [
        {"id": 1, "content_type": "article", "title": "A"},
        {"id": 1, "content_type": "blog", "title": "B"},
    ]
    out = _dedup_by_content_type_id(records)
    assert len(out) == 2
    titles = {r["title"] for r in out}
    assert titles == {"A", "B"}


def test_dedup_lista_vacia():
    assert _dedup_by_content_type_id([]) == []
