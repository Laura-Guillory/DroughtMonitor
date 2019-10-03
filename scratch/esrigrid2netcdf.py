import xarray
from dask.diagnostics import ProgressBar
import glob
from datetime import datetime

"""
This was written to convert a folder of data in ESRI gridded format and convert it into a single netCDF file.
The files are expected have no file extension and to be named according to the date they apply to.
For example:

D:/data/NDVI/1992010119920131
D:/data/NDVI/1992020119920229
D:/data/NDVI/1992030119920331

Will correspond to a netCDF file with three time entries for 01-01-1992, 01-02-1992 and 01-03-1992.
It will be saved in the working directory as 'result.nc'. Values of 99999.9 and -9999.9 will be filtered out.

This script is tailored for NDVI data from the Bureau of Meteorology, but will be improved in the future, so it can be 
configured for general purpose use.
"""

DIRECTORY = 'D:/data/NDVI'

file_paths = glob.glob(DIRECTORY + '/*')
times = []
for file_path in file_paths:
    file_name = file_path.split('\\')[-1]
    date = file_name.split('.')[0][:8]
    datetime_object = datetime.strptime(date, '%Y%m%d')
    times.append(datetime_object)
time_dim = xarray.Variable('time', times)
data_array = xarray.concat([xarray.open_rasterio(f) for f in file_paths], dim=time_dim)
dataset = data_array.to_dataset(name='ndvi').squeeze(drop=True).rename({'x': 'lon', 'y': 'lat'})
dataset = dataset.where(dataset.ndvi != 99999.9)
dataset = dataset.where(dataset.ndvi != -9999.9)
encoding = {}
for key in dataset.keys():
    encoding[key] = {'zlib': True}
delayed_obj = dataset.to_netcdf('result.nc', compute=False, format='NETCDF4', engine='netcdf4', unlimited_dims='time',
                                encoding=encoding)
with ProgressBar():
    delayed_obj.compute()
