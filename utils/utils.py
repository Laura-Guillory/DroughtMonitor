

def get_lon_lat_names(dataset):
    """
    Try to guess what the longitude and latitude dimensions are called and raise an exception if it doesn't work.
    """
    if all([element in dataset.dims for element in ['lon', 'lat']]):
        return 'lon', 'lat'
    elif all([element in dataset.dims for element in ['longitude', 'latitude']]):
        return 'longitude', 'latitude'
    raise ValueError('Longitude and/or latitude dimensions not found.')