import netCDF4


def get_lon_lat_names(dataset: netCDF4.Dataset):
    """
    Try to guess what the longitude and latitude dimensions are called and raise an exception if it doesn't work.
    """
    if all([element in dataset.dimensions for element in ['lon', 'lat']]):
        return 'lon', 'lat'
    elif all([element in dataset.dimensions for element in ['longitude', 'latitude']]):
        return 'longitude', 'latitude'
    raise ValueError('Longitude and/or latitude dimensions not found.')