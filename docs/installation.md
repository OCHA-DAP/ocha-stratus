# Installation

## From PyPI

Install ocha-stratus using pip:

```bash
pip install ocha-stratus
```

## Development Installation

For development, install from source:

1. Clone the repository:
```bash
git clone https://github.com/OCHA-DAP/ocha-stratus.git
cd ocha-stratus
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install development dependencies:
```bash
pip install -e ".[dev]"
```

## Environment Configuration

This package depends on the following environment variables:

```bash
# Development Environment
DSCI_AZ_BLOB_DEV_SAS=your_dev_sas_token
DSCI_AZ_DB_DEV_PW=your_dev_db_password
DSCI_AZ_DB_DEV_HOST=your_dev_db_host
DSCI_AZ_DB_DEV_UID=your_dev_db_uid

# Production Environment
DSCI_AZ_BLOB_PROD_SAS=your_prod_sas_token
DSCI_AZ_DB_PROD_PW=your_prod_db_password
DSCI_AZ_DB_PROD_HOST=your_prod_db_host
DSCI_AZ_DB_PROD_UID=your_prod_db_uid
```

## Dependencies

ocha-stratus requires Python 3.10 or later and depends on:

- pandas
- geopandas
- xarray
- rioxarray
- azure-storage-blob
- sqlalchemy
- psycopg2-binary
- python-dotenv
- pyarrow
- dask

These will be installed automatically when you install ocha-stratus.
