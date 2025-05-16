from pathlib import Path
import re

import xarray as xr
import dask.dataframe as dd

xrds = xr.open_dataset(Path("../AQS_20230101_20240101.nc"), engine="netcdf4")

print(xrds)
