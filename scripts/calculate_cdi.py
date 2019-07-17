import argparse
import xarray
from datetime import datetime
from dask.diagnostics import ProgressBar
import os


"""
Script to calculate a Combined Drought Indicator for Australia.
Expects input of four netCDF files: Normalised Difference Vegetation Index, Short Crop Evapotranspiration, 2-month SPI,
and Available Water Content (Rootzone Soil Moisture). All files should be pre-processed into percentiles.

Will only calculate when data for all four inputs are available. If a date is missing data for one index, that date will
be skipped. If a location is missing data for one index, it will be filled in with NaN values.

Work in progress, would like to make this script do the pre-processing too.
"""

NDVI_WEIGHT = 0.2
SPI_WEIGHT = 0.4
ET_WEIGHT = 0.2
AWC_WEIGHT = 0.2


def main():
    start_time = datetime.now()
    print('Starting time: ' + str(start_time))

    options = get_options()
    with xarray.open_mfdataset([options.ndvi, options.spi, options.et, options.awc], chunks={'time': 20}) as data:
        save_to_disk(calc_cdi(data, options), options.output)
    end_time = datetime.now()
    print('End time: ' + str(end_time))
    elapsed_time = end_time - start_time
    print('Elapsed time: ' + str(elapsed_time))


def get_options():
    """
    Gets command line arguments and returns them.
    Options are accessed via options.ndvi, options.output, etc.

    Required arguments: ndvi, spi, et, awc, output

    Run this with the -h (help) argument for more detailed information. (python calculate_cdi.py -h)

    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--ndvi',
        help='The path to the input NDVI netCDF file.',
        required=True
    )
    parser.add_argument(
        '--ndvi_var',
        help='The name of the NDVI variable in the netCDF file.',
        required=True
    )
    parser.add_argument(
        '--spi',
        help='The path to the input SPI netCDF file.',
        required=True
    )
    parser.add_argument(
        '--spi_var',
        help='The name of the SPI variable in the netCDF file.',
        required=True
    )
    parser.add_argument(
        '--et',
        help='The path to the input Evapotranspiration netCDF file.',
        required=True
    )
    parser.add_argument(
        '--et_var',
        help='The name of the Evapotranspiration variable in the netCDF file.',
        required=True
    )
    parser.add_argument(
        '--awc',
        help='The path to the input Available Water Content netCDF file.',
        required=True
    )
    parser.add_argument(
        '--awc_var',
        help='The name of the Available Water Content variable in the netCDF file.',
        required=True
    )
    parser.add_argument(
        '--output',
        help='The location to save the result.',
        required=True
    )
    args = parser.parse_args()
    return args


def calc_cdi(dataset, options):
    dataset['cdi'] = NDVI_WEIGHT * dataset[options.ndvi_var] \
                     + SPI_WEIGHT * dataset[options.spi_var] \
                     + ET_WEIGHT * (1 - dataset[options.et_var]) \
                     + AWC_WEIGHT * dataset[options.awc_var]
    dataset = dataset.drop([options.ndvi_var, options.spi_var, options.et_var, options.awc_var])
    dataset = dataset.dropna('time', how='all')
    return dataset


def save_to_disk(dataset, file_path, encoding=None):
    print('Saving: ' + file_path)
    if len(os.path.dirname(file_path)) > 0:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if not encoding:
        encoding = {}
    for key in dataset.keys():
        encoding[key] = {'zlib': True}
    delayed_obj = dataset.to_netcdf(file_path, compute=False, format='NETCDF4', engine='netcdf4',
                                    unlimited_dims='time', encoding=encoding)
    with ProgressBar():
        delayed_obj.compute()


if __name__ == '__main__':
    main()
