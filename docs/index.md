# ocha-stratus Documentation

```{toctree}
:maxdepth: 3
:caption: Contents

installation
api
```

## Overview

ocha-stratus is a Python package developed by the [Data Science team](https://centre.humdata.org/data-science/) at the [Centre for Humanitarian Data](https://centre.humdata.org/) for basic data access and storage operations against internally-managed Azure cloud infrastructure. It provides utilities for:

- Reading and writing various data formats to Azure Blob Storage:
  - Parquet files
  - CSV files
  - Shapefiles
  - Cloud Optimized GeoTIFFs (COGs)
- Managing Azure PostgreSQL database connections and operations
- Supporting development and production environments

## Quick Start

Install the package:
```bash
pip install ocha-stratus
```

### Azure Blob Storage

```python
import ocha_stratus as stratus

# Upload a pandas DataFrame as CSV
stratus.upload_csv_to_blob(df, "data.csv", stage="dev")

# Load it back
df = stratus.load_csv_from_blob("data.csv", stage="dev")
```

### Azure PostgreSQL Database

```python
import ocha_stratus as stratus

# Get database connection
engine = stratus.get_engine(stage="dev")

# Perform upsert operation
stratus.postgres_upsert(table, conn, keys, data_iter)
```

### Load COGs and clip by a GeoDataFrame

```python
import ocha_stratus as stratus

gdf = stratus.load_codab_from_blob(
  iso3="NGA",
  admin_level=0
)

date_range = ["2024-01-01", "2024-02-01", "2024-03-01"]
ds = stratus.stack_cogs("era5", date_range, "dev", clip_gdf=gdf)
```

## Environment Configuration

This package depends on the following environment variables:

```bash
# Development Environment
DSCI_AZ_BLOB_DEV_SAS=your_dev_sas_token
DSCI_AZ_DB_DEV_PW=your_dev_db_password
DSCI_AZ_DB_DEV_UID=your_dev_db_uid

DSCI_AZ_BLOB_DEV_SAS_WRITE=your_dev_sas_token_w_write_permissions
DSCI_AZ_DB_DEV_PW_WRITE=your_dev_db_password_w_write_permissions
DSCI_AZ_DB_DEV_UID_WRITE=your_dev_db_uid_w_write_permissions

DSCI_AZ_DB_DEV_HOST=your_dev_db_host

# Production Environment
DSCI_AZ_BLOB_PROD_SAS=your_prod_sas_token
DSCI_AZ_DB_PROD_PW=your_prod_db_password
DSCI_AZ_DB_PROD_UID=your_prod_db_uid

DSCI_AZ_BLOB_PROD_SAS_WRITE=your_prod_sas_token_w_write_permissions
DSCI_AZ_DB_PROD_PW_WRITE=your_prod_db_password_w_write_permissions
DSCI_AZ_DB_PROD_UID_WRITE=your_prod_db_uid_w_write_permissions

DSCI_AZ_DB_PROD_HOST=your_prod_db_host
```
