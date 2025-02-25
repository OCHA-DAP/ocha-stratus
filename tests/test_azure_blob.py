from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from azure.storage.blob import ContainerClient


@pytest.fixture
def mock_container_client():
    """Create a mock container client."""
    with patch("ocha_stratus.azure_blob.ContainerClient") as mock:
        client = MagicMock(spec=ContainerClient)
        mock.from_container_url.return_value = client
        yield client


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})


def test_get_container_client_dev():
    """Test getting a dev container client."""
    with patch("dotenv.load_dotenv"):
        from ocha_stratus.azure_blob import get_container_client

        with patch("ocha_stratus.azure_blob.ContainerClient") as mock:
            get_container_client(stage="dev")
            mock.from_container_url.assert_called_once()
            url = mock.from_container_url.call_args[0][0]
            assert "dev" in url
            assert "fake-dev-sas" in url  # Verify the mocked SAS token is used


def test_get_container_client_prod():
    """Test getting a prod container client."""
    with patch("dotenv.load_dotenv"):
        from ocha_stratus.azure_blob import get_container_client

        with patch("ocha_stratus.azure_blob.ContainerClient") as mock:
            get_container_client(stage="prod")
            mock.from_container_url.assert_called_once()
            url = mock.from_container_url.call_args[0][0]
            assert "prod" in url
            assert "fake-prod-sas" in url


def test_get_container_client_invalid_stage():
    """Test getting a container client with invalid stage."""
    with patch("dotenv.load_dotenv"):
        from ocha_stratus.azure_blob import get_container_client

        with pytest.raises(ValueError):
            get_container_client(stage="invalid")


def test_upload_parquet_to_blob(mock_container_client, sample_dataframe):
    """Test uploading a parquet file to blob storage."""
    with patch("dotenv.load_dotenv"):
        from ocha_stratus.azure_blob import upload_parquet_to_blob

        blob_name = "test.parquet"
        upload_parquet_to_blob(sample_dataframe, blob_name, stage="dev")
        mock_container_client.get_blob_client.assert_called_once_with(
            blob_name
        )
        mock_container_client.get_blob_client.return_value.upload_blob.assert_called_once()


def test_load_parquet_from_blob(mock_container_client, sample_dataframe):
    """Test loading a parquet file from blob storage."""
    with patch("dotenv.load_dotenv"):
        from ocha_stratus.azure_blob import load_parquet_from_blob

        # Create parquet data
        parquet_data = sample_dataframe.to_parquet()
        mock_container_client.get_blob_client.return_value.download_blob.return_value.readall.return_value = parquet_data

        # Load data
        result = load_parquet_from_blob("test.parquet", stage="dev")
        pd.testing.assert_frame_equal(result, sample_dataframe)


def test_upload_csv_to_blob(mock_container_client, sample_dataframe):
    """Test uploading a CSV file to blob storage."""
    with patch("dotenv.load_dotenv"):
        from ocha_stratus.azure_blob import upload_csv_to_blob

        blob_name = "test.csv"
        upload_csv_to_blob(sample_dataframe, blob_name, stage="dev")
        mock_container_client.get_blob_client.assert_called_once_with(
            blob_name
        )
        mock_container_client.get_blob_client.return_value.upload_blob.assert_called_once()


def test_load_csv_from_blob(mock_container_client, sample_dataframe):
    """Test loading a CSV file from blob storage."""
    with patch("dotenv.load_dotenv"):
        from ocha_stratus.azure_blob import load_csv_from_blob

        # Create CSV data
        csv_data = sample_dataframe.to_csv(index=False).encode()
        mock_container_client.get_blob_client.return_value.download_blob.return_value.readall.return_value = csv_data

        # Load data
        result = load_csv_from_blob("test.csv", stage="dev")
        pd.testing.assert_frame_equal(result, sample_dataframe)
