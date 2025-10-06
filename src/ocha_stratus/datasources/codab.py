from typing import Literal

import geopandas as gpd
from fsspec.implementations.http import HTTPFileSystem

from ..azure_blob import load_shp_from_blob

GEOPARQUET_URLS = {
    0: "https://data.fieldmaps.io/adm0/osm/intl/adm0_polygons.parquet",
    1: "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet",
    2: "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm2_polygons.parquet",
}


def load_codab_from_fieldmaps(
    iso3: str,
    admin_level: int = 0,
) -> gpd.GeoDataFrame:
    """
    Load COD-AB boundaries directly into memory from FieldMaps GeoParquet files.
    Data is from the global edge-matched subnational boundary layers here:
    https://fieldmaps.io/data.

    Parameters
    ----------
    iso3 : str
        ISO 3166-1 alpha-3 country code
    admin_level : int, optional
        Administrative level (0, 1, or 2), by default 0

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing administrative boundaries for the specified country and level
    """
    iso3 = iso3.upper()
    url = GEOPARQUET_URLS[admin_level]
    filters = [("iso_3", "=", iso3)]
    filesystem = HTTPFileSystem()
    return gpd.read_parquet(url, filters=filters, filesystem=filesystem)


def load_codab_from_blob(
    iso3: str, admin_level: int = 0, stage: Literal["dev", "prod"] = "prod"
) -> gpd.GeoDataFrame:
    """
    Load COD-AB boundaries from Fieldmaps cached in Azure Blob Storage.
    Data downloaded from https://fieldmaps.io/data/cod.

    Parameters
    ----------
    iso3 : str
        ISO 3166-1 alpha-3 country code
    admin_level : int, optional
        Administrative level (0, 1, or 2), by default 0
    stage : Literal["dev", "prod"], optional
        Environment stage to load from, by default "prod"

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing administrative boundaries for the specified country and level
    """
    iso3 = iso3.lower()
    shapefile = f"{iso3}_adm{admin_level}.shp"
    gdf = load_shp_from_blob(
        container_name="polygon",
        blob_name=f"{iso3.lower()}_shp.zip",
        shapefile=shapefile,
        stage=stage,
    )
    return gdf
