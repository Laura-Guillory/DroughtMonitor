import xarray
import argparse
from utils import save_to_netcdf


"""
To retrieve specific time slices from netCDF files
"""

time_start = '1911-01'
time_end = '2019-08'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-input', help='The file to read data from.', required=True)
    parser.add_argument('-output', help='The file to save the output to.', required=True)
    args = parser.parse_args()
    with xarray.open_dataset(args.input) as dataset:
        subset = dataset.sel(time=slice(time_start, time_end))
        save_to_netcdf(subset, args.output)


if __name__ == '__main__':
    main()
