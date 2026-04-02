from unittest.mock import MagicMock
from shared.network import fetch_with_retry, create_session


def test_create_session_sets_user_agent():
    session = create_session()
    assert "heroincome-data" in session.headers["User-Agent"]


def test_fetch_with_retry_returns_none_on_404():
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response
    result = fetch_with_retry(mock_session, "https://example.com/404")
    assert result is None


def test_fetch_with_retry_returns_response_on_200():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response
    result = fetch_with_retry(mock_session, "https://example.com/ok")
    assert result is mock_response


import json
import os
import tempfile
from shared.io import save_json, update_index
from shared.dates import parse_date_dmy, parse_date_iso


def test_save_json_creates_file_with_content():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "sub", "test.json")
        save_json(path, {"key": "value"})
        with open(path, encoding="utf-8") as f:
            assert json.load(f) == {"key": "value"}


def test_save_json_handles_cyrillic():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test.json")
        save_json(path, {"name": "Лукойл"})
        with open(path, encoding="utf-8") as f:
            content = f.read()
        assert "Лукойл" in content


def test_update_index_creates_index_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "index.json")
        update_index(path, ["SBER", "LKOH"])
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        assert data["tickersCount"] == 2
        assert data["tickers"] == ["LKOH", "SBER"]
        assert "updatedAt" in data


def test_parse_date_dmy_valid():
    assert parse_date_dmy("21.11.2025") == "2025-11-21"


def test_parse_date_dmy_invalid():
    assert parse_date_dmy("n/a") is None
    assert parse_date_dmy("") is None
    assert parse_date_dmy("  ") is None


def test_parse_date_iso_valid():
    assert parse_date_iso("2025-11-21") == "2025-11-21"


def test_parse_date_iso_invalid():
    assert parse_date_iso("not-a-date") is None
