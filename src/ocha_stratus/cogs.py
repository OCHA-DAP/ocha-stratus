import logging
import re
from typing import List, Literal, Optional, Union

import geopandas as gpd
import pandas as pd
import rioxarray  # noqa
import tqdm
import xarray as xr

from .azure_blob import get_container_client, open_blob_cog

logger = logging.getLogger(__name__)


def _parse_date(filename):
    """
    Parses the date based on a COG filename.
    """
    res = re.search("([0-9]{4}-[0-9]{2}-[0-9]{2})", filename)
    try:
        output_date = pd.to_datetime(res[0]).strftime("%Y-%m-%d")
        return output_date
    except Exception:
        return None


def stack_cogs(
    dataset: Literal["imerg", "seas5", "era5", "floodscan"],
    dates: Union[List[str], List],
    stage: str = "prod",
    clip_gdf: Optional[gpd.GeoDataFrame] = None,
    mode: Literal["interactive", "pipeline"] = "interactive",
) -> xr.Dataset:
    """
    Stack Cloud Optimized GeoTIFFs (COGs) from Azure Blob Storage into a single xarray Dataset.

    Retrieves and combines multiple COG files for a specified dataset and date range
    from Azure Blob Storage, returning a unified xarray Dataset with temporal and
    optional leadtime dimensions.

    Parameters
    ----------
    dataset : {"imerg", "seas5", "era5", "floodscan"}
        Name of the dataset to retrieve COGs for. Used as prefix for blob name filtering.
    dates : List[str] or List
        Collection of dates to filter COGs by. Should match 'YYYY-MM-DD' format.
        Will reference the issued date of the dataset (although for non-forecast
        datasets this is equivalent to the valid date).
    clip_gdf : GeoDataFrame, optional
        GeoPandas DataFrame containing geometries to clip the COGs to. If provided,
        each COG will be clipped to the union of all geometries in the DataFrame
        before stacking. The GeoDataFrame should be in the same CRS as the COGs.
    stage : str, optional
        Deployment stage for the container client, by default "prod".
        Determines which Azure storage environment to connect to.
    mode : {"interactive", "pipeline"}, optional
        Processing mode, by default "interactive". If "interactive", displays
        a progress bar using tqdm during processing.

    Returns
    -------
    xarray.Dataset
        Combined dataset with all COGs stacked along temporal dimensions.
        Contains 'date' dimension and optional 'leadtime' dimension if present
        in the source data. Attributes from individual COGs are dropped during
        combination. If clip_gdf is provided, data will be clipped to the specified
        geometries.

    Raises
    ------
    Exception
        If no COGs are found matching the specified dataset and dates.

    Warnings
    --------
    Logs a warning if the number of found COGs doesn't match the number of
    input dates, indicating some requested dates may not have available data.

    Notes
    -----
    - Only processes COGs containing "processed" in their filename
    - Handles both issued and valid date types based on COG metadata
    - Automatically expands dimensions to include 'date' and 'leadtime' (if present)
    - Uses `xr.combine_by_coords` to merge datasets, which requires consistent
    coordinate systems across all input COGs
    """

    container = get_container_client("raster", stage=stage)

    clip_geometry = None
    if clip_gdf is not None:
        # Union all geometries in the GeoDataFrame
        clip_geometry = clip_gdf.geometry.unary_union
        logger.info(f"Clipping enabled with {len(clip_gdf)} geometries")

    cogs_list = [
        x.name
        for x in container.list_blobs(name_starts_with=f"{dataset}/")
        if (_parse_date(x.name) in (dates)) & ("processed" in x.name)
    ]

    das = []
    cogs_list = tqdm.tqdm(cogs_list) if mode == "interactive" else cogs_list
    logger.info(f"Stacking {len(cogs_list)} cogs...")

    if len(cogs_list) != len(dates):
        logger.warning("Not all COGs available, given input dates")
    if len(cogs_list) == 0:
        raise Exception(f"No COGs found to process for dates: {dates}")

    for cog in cogs_list:
        da_in = open_blob_cog(
            cog, container_name="raster", container_client=container
        )

        if clip_geometry is not None:
            da_in = da_in.rio.clip([clip_geometry], drop=True)
        date_suffix = (
            "valid" if da_in.attrs["month_issued"] == "None" else "issued"
        )
        year_ = da_in.attrs[f"year_{date_suffix}"]
        month_ = str(da_in.attrs[f"month_{date_suffix}"]).zfill(2)
        day_ = (
            "01"
            if da_in.attrs["date_valid"] == "None"
            else da_in.attrs["date_valid"]
        )
        date_in = f"{year_}-{month_}-{day_}"
        da_in = da_in.squeeze(drop=True)
        da_in["date"] = date_in
        expand_dims = ["date"]
        if da_in.attrs["leadtime"] != "None":
            da_in["leadtime"] = da_in.attrs["leadtime"]
            expand_dims.append("leadtime")
        da_in = da_in.expand_dims(expand_dims)
        da_in = da_in.persist()

        das.append(da_in)

    return xr.combine_by_coords(das, combine_attrs="drop")
