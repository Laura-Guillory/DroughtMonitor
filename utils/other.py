import netCDF4
import xarray


def get_lon_lat_names(dataset: netCDF4.Dataset):
    """
    Try to guess what the longitude and latitude dimensions are called and raise an exception if it doesn't work.
    """
    if type(dataset) is netCDF4.Dataset:
        dimensions = dataset.dimensions
    elif type(dataset) is xarray.Dataset:
        dimensions = dataset.dims
    else:
        raise ValueError('Must be given an xarray or netCDF4 Dataset.')
    if all([element in dimensions for element in ['lon', 'lat']]):
        return 'lon', 'lat'
    elif all([element in dimensions for element in ['longitude', 'latitude']]):
        return 'longitude', 'latitude'
    raise ValueError('Longitude and/or latitude dimensions not found.')