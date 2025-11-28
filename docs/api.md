# API Reference

## Azure Blob Storage

Utilities for working with Azure Blob Storage.

### Container Operations

```{eval-rst}
.. autofunction:: ocha_stratus.get_container_client
.. autofunction:: ocha_stratus.list_container_blobs
```

### File Operations

#### CSV Files
```{eval-rst}
.. autofunction:: ocha_stratus.upload_csv_to_blob
.. autofunction:: ocha_stratus.load_csv_from_blob
```

#### Parquet Files
```{eval-rst}
.. autofunction:: ocha_stratus.upload_parquet_to_blob
.. autofunction:: ocha_stratus.load_parquet_from_blob
.. autofunction:: ocha_stratus.load_geoparquet_from_blob
```

#### Shapefiles
```{eval-rst}
.. autofunction:: ocha_stratus.upload_shp_to_blob
.. autofunction:: ocha_stratus.load_shp_from_blob
```

#### Cloud Optimized GeoTIFFs
```{eval-rst}
.. autofunction:: ocha_stratus.upload_cog_to_blob
.. autofunction:: ocha_stratus.open_blob_cog
```

#### Generic data
```{eval-rst}
.. autofunction:: ocha_stratus.upload_blob_data
.. autofunction:: ocha_stratus.load_blob_data
```

## Database Operations

Utilities for working with Azure PostgreSQL databases.

```{eval-rst}
.. autofunction:: ocha_stratus.get_engine
.. autofunction:: ocha_stratus.postgres_upsert
```

## Cloud-Optimized GeoTIFF (COG) Operations

Utilities for working with standard COG datasets.

```{eval-rst}
.. autofunction:: ocha_stratus.stack_cogs
```

## Datasets

Dataset-specific loading functions.

### Administrative boundaries
```{eval-rst}
.. autofunction:: ocha_stratus.codab.load_codab_from_blob
.. autofunction:: ocha_stratus.codab.load_codab_from_fieldmaps
```

### CERF funding allocations
```{eval-rst}
.. autofunction:: ocha_stratus.cerf.load_cerf_from_blob
```

### EM-DAT disaster records
```{eval-rst}
.. autofunction:: ocha_stratus.emdat.load_emdat_from_blob
```
