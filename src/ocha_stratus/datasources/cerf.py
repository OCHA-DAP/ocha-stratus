import logging
from typing import Literal

import duckdb
import pandas as pd

from ..azure_blob import get_container_client

logger = logging.getLogger(__name__)

CERF_FNAME = "cerf/cerf_hdx_download.parquet"


def load_cerf_from_blob(
    iso3: str | None = None, stage: Literal["dev", "prod"] = "dev"
) -> pd.DataFrame:
    """
    Load CERF funding data from Azure blob storage.

    Retrieves CERF (Central Emergency Response Fund) data stored as a Parquet
    file in Azure blob storage, with optional filtering by country ISO3 code.
    Data downloaded from https://data.humdata.org/dataset/cerf-allocations and
    manually transformed to parquet and uploaded to blob.

    Parameters
    ----------
    iso3 : str or None, optional
        ISO3 country code to filter results. If None, returns all records.
        Default is None.
    stage : Literal["dev", "prod"], optional
        Environment stage to load from, by default "dev"

    Returns
    -------
    pd.DataFrame
        DataFrame containing CERF funding data, optionally filtered by country.
    """
    iso3 = iso3.upper() if iso3 else iso3
    blob_client = get_container_client(
        container_name="global", stage=stage
    ).get_blob_client(CERF_FNAME)
    url = blob_client.url

    blob_properties = blob_client.get_blob_properties()
    last_modified = blob_properties.last_modified
    logger.info(f"CERF data last updated: {last_modified}")

    con = duckdb.connect()
    if iso3 is not None:
        df = con.execute(
            f"SELECT * FROM read_parquet('{url}') WHERE countryCode = $1",
            [iso3],
        ).df()
    else:
        df = con.execute(f"SELECT * FROM read_parquet('{url}')").df()
    return df
