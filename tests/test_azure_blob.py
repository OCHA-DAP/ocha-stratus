from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest
import xarray as xr
from azure.storage.blob import ContainerClient
from shapely.geometry import Point


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


@pytest.fixture
def sample_xarray():
    """Create a sample DataArray for testing."""
    return xr.DataArray([[1, 2], [3, 4]])


@pytest.fixture
def sample_geodataframe():
    """Create a sample GeoDataFrame for testing."""
    gdf = gpd.GeoDataFrame(
        {"id": [1, 2, 3], "value": ["a", "b", "c"]},
        geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
        crs="EPSG:4326",
    )
    return gdf


def test_get_container_client_read_dev():
    """Test getting a dev container client for reading."""
    from ocha_stratus.azure_blob import get_container_client

    with patch("ocha_stratus.azure_blob.ContainerClient") as mock:
        get_container_client(stage="dev", write=False)
        mock.from_container_url.assert_called_once()
        url = mock.from_container_url.call_args[0][0]
        assert "dev" in url
        assert "fake-dev-sas" in url  # Regular SAS token
        assert "fake-dev-sas-write" not in url  # Not using write token


def test_get_container_client_write_dev():
    """Test getting a dev container client for writing."""
    from ocha_stratus.azure_blob import get_container_client

    with patch("ocha_stratus.azure_blob.ContainerClient") as mock:
        get_container_client(stage="dev", write=True)
        mock.from_container_url.assert_called_once()
        url = mock.from_container_url.call_args[0][0]
        assert "dev" in url
        assert "fake-dev-sas-write" in url  # Using write token


def test_get_container_client_prod():
    """Test getting a prod container client."""
    from ocha_stratus.azure_blob import get_container_client

    with patch("ocha_stratus.azure_blob.ContainerClient") as mock:
        get_container_client(stage="prod")
        mock.from_container_url.assert_called_once()
        url = mock.from_container_url.call_args[0][0]
        assert "prod" in url
        assert "fake-prod-sas" in url


def test_get_container_client_invalid_stage():
    """Test getting a container client with invalid stage."""
    from ocha_stratus.azure_blob import get_container_client

    with pytest.raises(ValueError):
        get_container_client(stage="invalid")


def test_upload_parquet_to_blob(mock_container_client, sample_dataframe):
    """Test uploading a parquet file to blob storage."""
    from ocha_stratus.azure_blob import upload_parquet_to_blob

    blob_name = "test.parquet"
    upload_parquet_to_blob(sample_dataframe, blob_name, stage="dev")
    mock_container_client.get_blob_client.assert_called_once_with(blob_name)
    mock_container_client.get_blob_client.return_value.upload_blob.assert_called_once()


def test_upload_parquet_to_blob_geodataframe(
    mock_container_client, sample_geodataframe
):
    """Test uploading a GeoDataFrame to blob storage in parquet format."""
    from ocha_stratus.azure_blob import upload_parquet_to_blob

    blob_name = "test_geo.parquet"
    upload_parquet_to_blob(sample_geodataframe, blob_name, stage="dev")
    mock_container_client.get_blob_client.assert_called_once_with(blob_name)
    mock_container_client.get_blob_client.return_value.upload_blob.assert_called_once()

    # Verify that bytes were passed to upload_blob
    call_args = (
        mock_container_client.get_blob_client.return_value.upload_blob.call_args
    )
    uploaded_data = call_args[0][0]
    assert isinstance(uploaded_data, bytes)


def test_load_parquet_from_blob(mock_container_client, sample_dataframe):
    """Test loading a parquet file from blob storage."""
    from ocha_stratus.azure_blob import load_parquet_from_blob

    # Create parquet data
    parquet_data = sample_dataframe.to_parquet()
    mock_container_client.get_blob_client.return_value.download_blob.return_value.readall.return_value = parquet_data

    # Load data
    result = load_parquet_from_blob("test.parquet", stage="dev")
    pd.testing.assert_frame_equal(result, sample_dataframe)


def test_load_geoparquet_from_blob(mock_container_client, sample_geodataframe):
    """Test loading a GeoParquet file from blob storage."""
    import io

    from ocha_stratus.azure_blob import load_geoparquet_from_blob

    # Create GeoParquet data
    buffer = io.BytesIO()
    sample_geodataframe.to_parquet(buffer)
    geoparquet_data = buffer.getvalue()
    mock_container_client.get_blob_client.return_value.download_blob.return_value.readall.return_value = geoparquet_data

    # Load data
    result = load_geoparquet_from_blob("test.parquet", stage="dev")

    # Verify result is a GeoDataFrame
    assert isinstance(result, gpd.GeoDataFrame)

    # Verify CRS is preserved
    assert result.crs == sample_geodataframe.crs

    # Verify data is the same
    pd.testing.assert_frame_equal(result, sample_geodataframe)


def test_upload_csv_to_blob(mock_container_client, sample_dataframe):
    """Test uploading a CSV file to blob storage."""
    from ocha_stratus.azure_blob import upload_csv_to_blob

    blob_name = "test.csv"
    upload_csv_to_blob(sample_dataframe, blob_name, stage="dev")
    mock_container_client.get_blob_client.assert_called_once_with(blob_name)
    mock_container_client.get_blob_client.return_value.upload_blob.assert_called_once()


def test_load_csv_from_blob(mock_container_client, sample_dataframe):
    """Test loading a CSV file from blob storage."""
    from ocha_stratus.azure_blob import load_csv_from_blob

    # Create CSV data
    csv_data = sample_dataframe.to_csv(index=False).encode()
    mock_container_client.get_blob_client.return_value.download_blob.return_value.readall.return_value = csv_data

    # Load data
    result = load_csv_from_blob("test.csv", stage="dev")
    pd.testing.assert_frame_equal(result, sample_dataframe)


def test_upload_cog_to_blob(sample_xarray):
    """Test uploading a COG to blob storage uses write SAS token."""
    with patch(
        "ocha_stratus.azure_blob.get_container_client"
    ) as mock_get_client:
        mock_client = MagicMock()
        mock_blob_client = MagicMock()
        mock_client.get_blob_client.return_value = mock_blob_client
        mock_get_client.return_value = mock_client

        # Mock the necessary methods
        with (
            patch("tempfile.NamedTemporaryFile"),
            patch("builtins.open"),
            patch("xarray.DataArray.rio.to_raster"),
        ):
            from ocha_stratus.azure_blob import upload_cog_to_blob

            upload_cog_to_blob(sample_xarray, "test.tif", stage="dev")

            # Verify get_container_client was called with write=True
            mock_get_client.assert_called_with(
                container_name="projects", stage="dev", write=True
            )
