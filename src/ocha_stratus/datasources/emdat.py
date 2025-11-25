import logging
from typing import Literal

import duckdb
import pandas as pd

from ..azure_blob import get_container_client

logger = logging.getLogger(__name__)

EMDAT_FNAME = "emdat/processed/emdat_all.parquet"


def load_emdat_from_blob(
    iso3: str | None = None, stage: Literal["dev", "prod"] = "dev"
) -> pd.DataFrame:
    """
    Load EM-DAT disaster data from Azure blob storage.
    See here for a description of columns:
    https://doc.emdat.be/docs/data-structure-and-content/emdat-public-table/#column-description

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
        DataFrame containing EM-DAT disaster data, optionally filtered by country.
    """
    iso3 = iso3.upper() if iso3 else iso3
    blob_client = get_container_client(
        container_name="global", stage=stage
    ).get_blob_client(EMDAT_FNAME)
    url = blob_client.url

    blob_properties = blob_client.get_blob_properties()
    last_modified = blob_properties.last_modified
    logger.info(f"EMDAT data last updated: {last_modified}")

    con = duckdb.connect()
    if iso3 is not None:
        df = con.execute(
            f"SELECT * FROM read_parquet('{url}') WHERE ISO = $1",
            [iso3],
        ).df()
    else:
        df = con.execute(f"SELECT * FROM read_parquet('{url}')").df()
    return df
