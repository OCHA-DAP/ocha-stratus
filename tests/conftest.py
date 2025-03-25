import pytest


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Mock environment variables for testing."""
    env_vars = {
        "DSCI_AZ_BLOB_DEV_SAS": "fake-dev-sas",
        "DSCI_AZ_BLOB_DEV_SAS_WRITE": "fake-dev-sas-write",
        "DSCI_AZ_BLOB_PROD_SAS": "fake-prod-sas",
        "DSCI_AZ_DB_DEV_PW": "fake-dev-pw",
        "DSCI_AZ_DB_PROD_PW": "fake-prod-pw",
        "DSCI_AZ_DB_DEV_HOST": "fake-dev-host",
        "DSCI_AZ_DB_PROD_HOST": "fake-prod-host",
        "DSCI_AZ_DB_DEV_UID": "fake-dev-uid",
        "DSCI_AZ_DB_PROD_UID": "fake-prod-uid",
    }

    # Use monkeypatch instead of directly modifying os.environ
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
