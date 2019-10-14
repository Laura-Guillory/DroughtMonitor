import argparse
import xarray
from datetime import datetime
from utils import save_to_netcdf
import logging
from percentile_rank import percentile_rank
from truncate_time_dim import truncate_time_dim
import os
import numpy

logging.basicConfig(level=logging.WARN, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d  %H:%M:%S")
LOGGER = logging.getLogger(__name__)


"""
Script to calculate the Australian Combined Drought Indicator.
Expects input of four netCDF files: Normalised Difference Vegetation Index, Short Crop Evapotranspiration, 3-month SPI,
and Available Water Content (Rootzone Soil Moisture). Files do not need to be pre-processed into relative values or 
percentiles, actual values are ideal.

Data from all four inputs are required. In the event that one or more are missing for a coordinate, that coordinate will
be filled in with a NaN value. If one or more inputs are missing for certain dates, that date will be excluded from the 
result.
"""

NDVI_WEIGHT = 0.2
SPI_WEIGHT = 0.4
ET_WEIGHT = 0.2
SM_WEIGHT = 0.2


def main():
    options = get_options()
    LOGGER.setLevel(options.verbose)
    start_time = datetime.now()
    LOGGER.info('Starting time: ' + str(start_time))

    input_files = [
        {'var': options.ndvi_var, 'path': options.ndvi},
        {'var': options.sm_var, 'path': options.sm},
        {'var': options.et_var, 'path': options.et},
        {'var': options.spi_var, 'path': options.spi}
    ]

    # Percentile rank all the input data
    ranked_files = []
    for input_file in input_files:
        with xarray.open_dataset(input_file['path']) as dataset:
            ranked_dataset = percentile_rank(dataset, logging_level=options.verbose, rank_vars=[input_file['var']])
            temp_filepath = input_file['path'] + '.' + str(os.getpid()) + '.temp'
            save_to_netcdf(ranked_dataset, temp_filepath)
            ranked_files.append(temp_filepath)

    # Combine the percentile ranked data into a CDI
    with xarray.open_mfdataset(ranked_files, chunks={'time': 20}, preprocess=standardise_dataset, combine='by_coords') \
            as combined_dataset:
        result = calc_cdi(combined_dataset, options)
        save_to_netcdf(result, options.output, logging_level=options.verbose)

    # Remove temporary percentile ranked files
    for file in ranked_files:
        os.remove(file)

    end_time = datetime.now()
    LOGGER.info('End time: ' + str(end_time))
    elapsed_time = end_time - start_time
    LOGGER.info('Elapsed time: ' + str(elapsed_time))


def standardise_dataset(dataset):
    """
    Prepare each dataset to be merged via xarray.open_mfdataset().
    This makes sure that the latitude and longitude dimensions have the same names, and the dates are all truncated to
    the beginning of the month.

    :param dataset: The dataset to standardise
    :return: Standardized dataset
    """
    try:
        dataset = dataset.rename({'lat': 'latitude', 'lon': 'longitude'})
    except ValueError:
        pass
    dataset['latitude'] = dataset['latitude'].astype('f4')  # This fixes xarray dropping values to Nan in a stripe
    dataset['longitude'] = dataset['longitude'].astype('f4')  # pattern. The exact cause was never found.
    dataset = truncate_time_dim(dataset)
    return dataset


def get_options():
    """
    Gets command line arguments and returns them.
    Options are accessed via options.ndvi, options.output, etc.

    Required arguments: ndvi, spi, et, sm, output
    Optional arguments: verbose (v)

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
        '--sm',
        help='The path to the input Soil Moisture netCDF file.',
        required=True
    )
    parser.add_argument(
        '--sm_var',
        help='The name of the Soil Moisture variable in the netCDF file.',
        required=True
    )
    parser.add_argument(
        '--output',
        help='The location to save the result.',
        required=True
    )
    parser.add_argument(
        '-v', '--verbose',
        help='Increase output verbosity',
        action='store_const',
        const=logging.INFO,
        default=logging.WARN
    )
    args = parser.parse_args()
    return args


def calc_cdi(dataset, options):
    """
    Calculates the CDI based on the dataset containing all four variables.

    :param dataset: Dataset containing soil moisture, ndvi, evapotranspiration and spi
    :param options: Object containing command-line options, used to get the name of the input variables
    :return: Dataset containing only the calculated CDI
    """
    dataset['cdi'] = NDVI_WEIGHT * dataset[options.ndvi_var] \
                     + SPI_WEIGHT * dataset[options.spi_var] \
                     + ET_WEIGHT * (1 - dataset[options.et_var]) \
                     + SM_WEIGHT * dataset[options.sm_var]
    dataset = dataset.drop([options.ndvi_var, options.spi_var, options.et_var, options.sm_var])
    dataset = dataset.dropna('time', how='all')
    # For some reason latitude becomes a double while longitude remains a float... tidy that up.
    dataset['latitude'] = dataset['latitude'].astype('f4')
    dataset['latitude'].attrs['units'] = 'degrees_north'
    dataset['longitude'].attrs['units'] = 'degrees_east'
    return dataset


if __name__ == '__main__':
    main()
