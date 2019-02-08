import xarray
import argparse
from dask.diagnostics import ProgressBar


"""
To retrive specific time slices from netCDF files
"""

time_start = '2009-09'
time_end = '2009-09'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-input', help='The file to read data from.', required=True)
    parser.add_argument('-output', help='The file to save the output to.', required=True)
    args = parser.parse_args()
    with xarray.open_dataset(args.input) as dataset:
        encoding = {}
        for key in dataset.keys():
            encoding[key] = {'zlib': True}
        dataset.load()
        subset = dataset.sel(time=slice(time_start, time_end))
        print(subset)
        delayed_obj = subset.to_netcdf(args.output, compute=False, format='NETCDF4', engine='netcdf4',
                                       encoding=encoding)
        with ProgressBar():
            delayed_obj.compute()


if __name__ == '__main__':
    main()
