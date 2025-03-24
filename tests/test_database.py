from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.engine import Engine


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine."""
    with patch("ocha_stratus.azure_database.create_engine") as mock:
        engine = MagicMock(spec=Engine)
        mock.return_value = engine
        yield engine


@pytest.fixture
def sample_table():
    """Create a sample SQLAlchemy table for testing."""
    metadata = MetaData()
    return Table(
        "test_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("value", String),
    )


def test_get_engine_dev():
    """Test getting a dev database engine."""
    # Import get_engine inside the test to avoid early environment loading
    from ocha_stratus.azure_database import get_engine

    with patch("ocha_stratus.azure_database.create_engine") as mock:
        get_engine(stage="dev")
        mock.assert_called_once()
        url = mock.call_args[0][0]
        assert "dev" in url
        assert "fake-dev-pw" in url  # Verify the mocked password is used
        assert "fake-dev-host" in url  # Verify the mocked host is used
        assert "fake-dev-uid" in url  # Verify the mocked UID is used


def test_get_engine_prod():
    """Test getting a prod database engine."""
    from ocha_stratus.azure_database import get_engine

    with patch("ocha_stratus.azure_database.create_engine") as mock:
        get_engine(stage="prod")
        mock.assert_called_once()
        url = mock.call_args[0][0]
        assert "prod" in url
        assert "fake-prod-pw" in url
        assert "fake-prod-host" in url
        assert "fake-prod-uid" in url


def test_get_engine_invalid_stage():
    """Test getting an engine with invalid stage."""
    from ocha_stratus.azure_database import get_engine

    with pytest.raises(ValueError):
        get_engine(stage="invalid")
