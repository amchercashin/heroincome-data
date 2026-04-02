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
