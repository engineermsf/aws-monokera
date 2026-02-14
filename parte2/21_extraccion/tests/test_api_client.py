import requests
from unittest.mock import MagicMock, patch

from extraction.api_client import fetch_page, fetch_all_for_endpoint


def test_fetch_page_anade_content_type_y_sigue_next():
    session = MagicMock()
    session.get.return_value.json.return_value = {
        "results": [{"id": 1, "title": "X"}],
        "next": "https://api.example/v4/articles/?limit=2&offset=2",
        "count": 10,
    }
    session.get.return_value.raise_for_status = MagicMock()
    session.get.return_value.status_code = 200

    items, next_url, count = fetch_page("https://api.example/v4/articles/?limit=2&offset=0", session)
    assert len(items) == 1
    assert items[0]["id"] == 1
    assert "next" in session.get.return_value.json.return_value
    assert count == 10


def test_fetch_all_for_endpoint_anade_content_type():
    session = MagicMock()
    session.get.return_value.json.side_effect = [
        {"results": [{"id": 1, "title": "A"}], "next": None, "count": 1},
    ]
    session.get.return_value.raise_for_status = MagicMock()
    session.get.return_value.status_code = 200

    with patch("extraction.api_client.time.sleep"):
        items, total = fetch_all_for_endpoint("articles", session, base_url="https://api.test/v4")
    assert len(items) == 1
    assert items[0]["content_type"] == "article"
    assert total == 1
