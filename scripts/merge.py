import xarray
from dask.diagnostics import ProgressBar

dataset = xarray.open_mfdataset('data/daily_rain/*.daily_rain.nc')
delayed_obj = dataset.to_netcdf('data/full_daily_rain.nc', compute=False)

with ProgressBar():
    delayed_obj.compute()
