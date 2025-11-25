import urllib.request
from pathlib import Path
from tempfile import TemporaryDirectory

import xarray as xr
from pystac_client import Client

WORLDPOP_STAC_URL = "https://api.stac.worldpop.org"


def load_worldpop_from_stac(
    iso3: str,
    year: int = 2025,
    resolution: str = "100m",
    product: str = "pop",
    cache_dir: Path | str | None = None,
) -> xr.DataArray:
    """
    Get WorldPop population data for a country.

    Parameters
    ----------
    iso3 : str
        ISO3 country code (e.g., "CUB", "HTI", "JAM")
    year : int
        Year (2015-2030 available)
    resolution : str
        "100m" or "1km"
    product : str
        "pop" for total population, "agesex" for age/sex breakdown
    cache_dir : Path, optional
        Directory to cache downloaded files. If None, uses a temp directory.

    Returns
    -------
    xr.DataArray
    """
    client = Client.open(WORLDPOP_STAC_URL)

    search = client.search(
        filter={
            "op": "=",
            "args": [{"property": "Alpha-3 code"}, iso3.upper()],
        }
    )

    prefix = f"{iso3.lower()}_{product}_{year}"
    matching = [
        item
        for item in search.items()
        if item.id.startswith(prefix) and f"_{resolution}_" in item.id
    ]

    if not matching:
        raise ValueError(
            f"No data found for {iso3}, {product}, {year}, {resolution}"
        )

    item = matching[0]
    cog_url = item.assets["data"].href

    # Download the file (server doesn't support range requests)
    if cache_dir:
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        local_path = cache_dir / f"{item.id}.tif"
        if not local_path.exists():
            print(f"Downloading {item.id}...")
            urllib.request.urlretrieve(cog_url, local_path)
    else:
        # Use temp directory
        tmp_dir = TemporaryDirectory()
        local_path = Path(tmp_dir.name) / f"{item.id}.tif"
        print(f"Downloading {item.id}...")
        urllib.request.urlretrieve(cog_url, local_path)

    da = xr.open_dataarray(local_path, engine="rasterio")
    if da.ndim == 3 and da.shape[0] == 1:
        da = da.squeeze("band", drop=True)

    da.attrs.update(
        {
            "iso3": iso3.upper(),
            "year": year,
            "resolution": resolution,
            "product": product,
            "item_id": item.id,
            "source_url": cog_url,
        }
    )
    return da
