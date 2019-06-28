import xarray
from dask.diagnostics import ProgressBar

"""
Saves the dimensions of a netCDF file in a different order, because some programs will expect the dimensions to be
ordered a specific way and won't run without it.

lat, lon, time -> time, lat, lon

This script is tailored for a one-off conversion on my machine, but will be improved in the future, so it can be 
configured for general purpose use.
"""

imput = 'D:/data/et_short_crop/full_et_short_crop.nc'
output = 'D:/data/et_short_crop/transposed_et_short_crop.nc'


with xarray.open_dataset(input, chunks={'time': 20}) as dataset:
    dataset = dataset.transpose('time', 'lat', 'lon')
    print('Saving: ' + output)
    encoding = {}
    if not encoding:
        encoding = {}
    for key in dataset.keys():
        encoding[key] = {'zlib': True}
    delayed_obj = dataset.to_netcdf(output, compute=False, format='NETCDF4', engine='netcdf4',
                                    unlimited_dims='time', encoding=encoding)
    with ProgressBar():
        delayed_obj.compute()
