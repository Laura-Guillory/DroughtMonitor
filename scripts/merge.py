import xarray
from dask.diagnostics import ProgressBar

dataset_paths = [{'inputs': 'data/daily_rain/*.daily_rain.nc', 'output': 'data/daily_rain/full_daily_rain.nc'}]

for dataset_path in dataset_paths:
    dataset = xarray.open_mfdataset(dataset_path['inputs'])
    delayed_obj = dataset.to_netcdf(dataset_path['output'], compute=False)
    with ProgressBar():
        delayed_obj.compute()
