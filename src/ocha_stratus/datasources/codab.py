import logging
from typing import Literal

import geopandas as gpd
from azure.core.exceptions import ResourceNotFoundError
from fsspec.implementations.http import HTTPFileSystem
from pyogrio.errors import DataSourceError

from ..azure_blob import load_shp_from_blob

logger = logging.getLogger(__name__)

GEOPARQUET_URLS = {
    0: "https://data.fieldmaps.io/adm0/osm/intl/adm0_polygons.parquet",
    1: "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet",
    2: "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm2_polygons.parquet",
    3: "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm3_polygons.parquet",
    4: "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm4_polygons.parquet",
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
        Administrative level (0-4), by default 0

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing administrative boundaries for the specified country and level
    """
    iso3 = iso3.upper()
    try:
        url = GEOPARQUET_URLS[admin_level]
    except KeyError:
        logger.error(f"CODAB data for admin level {admin_level} not found")
        return
    filters = [("iso_3", "=", iso3)]
    filesystem = HTTPFileSystem()
    gdf = gpd.read_parquet(url, filters=filters, filesystem=filesystem)
    if len(gdf) == 0:
        logger.error(f"CODAB data for {iso3} not found")
        return
    return gdf


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
        Administrative level (0-4), by default 0
    stage : Literal["dev", "prod"], optional
        Environment stage to load from, by default "prod"

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing administrative boundaries for the specified country and level
    """
    iso3 = iso3.lower()
    shapefile = f"{iso3}_adm{admin_level}.shp"
    try:
        gdf = load_shp_from_blob(
            container_name="polygon",
            blob_name=f"{iso3.lower()}_shp.zip",
            shapefile=shapefile,
            stage=stage,
        )
    except ResourceNotFoundError:
        logger.error(f"CODAB data for {iso3} not found")
        return
    except DataSourceError:
        logger.error(f"CODAB data for admin level {admin_level} not found")
        return
    return gdf
