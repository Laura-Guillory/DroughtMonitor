import argparse
import xarray
from datetime import datetime
import utils
import logging
from percentile_rank import percentile_rank
import os
import multiprocessing
import numpy
import bottleneck

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

The steps taken to calculate the CDI are as follows:
1. Percentile rank all the input datasets (NDVI, ET, SM, SPI)
2. Do a weighted average of these four inputs
3. Percentile rank the resulting CDI
"""

NDVI_WEIGHT = 0.1106
SPI_WEIGHT = 0.3206
ET_WEIGHT = 0.2470
SM_WEIGHT = 0.3218


def main():
    options = get_options()
    LOGGER.setLevel(options.verbose)
    start_time = datetime.now()
    LOGGER.info('Starting time: ' + str(start_time))

    if options.multiprocessing == "single":
        number_of_worker_processes = 1
    elif options.multiprocessing == "all":
        number_of_worker_processes = multiprocessing.cpu_count()
    else:
        number_of_worker_processes = multiprocessing.cpu_count() - 1

    input_files = [
        {'var': options.ndvi_var, 'path': options.ndvi},
        {'var': options.sm_var, 'path': options.sm},
        {'var': options.et_var, 'path': options.et},
        {'var': options.spi_var, 'path': options.spi}
    ]

    # Percentile rank all the input data
    # This is multiprocessed with one dataset per process.
    # The ranked datasets are stored to temporary files. Otherwise we would need to find a way to share data between
    # the processes which is not ideal.
    ranked_files = []
    multiprocess_args = []
    for input_file in input_files:
        temp_filepath = input_file['path'] + '.' + str(os.getpid()) + '.temp'
        # Save args to be used in the multiprocessing pool below
        multiprocess_args.append((input_file['path'], temp_filepath, options.verbose, input_file['var']))
        ranked_files.append(temp_filepath)
    pool = multiprocessing.Pool(number_of_worker_processes)
    # Calls percentile_rank_dataset() with each process in the pool, using the arguments saved above
    pool.map(percentile_rank_dataset, multiprocess_args)
    pool.close()
    pool.join()

    # Combine the percentile ranked data into a CDI
    with xarray.open_mfdataset(ranked_files, chunks={'time': 10}, preprocess=standardise_dataset,
                               combine='by_coords') as combined_dataset:
        calc_cdi(combined_dataset, options)

    # Remove temporary percentile ranked files
    for file in ranked_files:
        os.remove(file)

    end_time = datetime.now()
    LOGGER.info('End time: ' + str(end_time))
    elapsed_time = end_time - start_time
    LOGGER.info('Elapsed time: ' + str(elapsed_time))


def percentile_rank_dataset(args):
    """
    Loads a dataset, percentile ranks it, and saves it back to disk.
    Can't have arguments directly with multiprocessing, they're packed as a tuple
    """
    input_path, output_path, verbosity, var = args
    with xarray.open_dataset(input_path, chunks={'time': 10}) as dataset:
        dataset = utils.truncate_time_dim(dataset)
        try:
            dataset = dataset.transpose('time', 'lat', 'lon')
        except ValueError:
            pass
        utils.save_to_netcdf(dataset, output_path)
    percentile_rank(output_path, logging_level=verbosity, rank_vars=[var])


def standardise_dataset(dataset: xarray.Dataset):
    """
    Prepare each dataset to be merged via xarray.open_mfdataset().
    This makes sure that the latitude and longitude dimensions have the same names.

    :param dataset: The dataset to standardise
    :return: Standardized dataset
    """
    try:
        dataset = dataset.rename({'lat': 'latitude', 'lon': 'longitude'})
    except ValueError:
        pass
    dataset['latitude'] = numpy.around(dataset['latitude'].values, decimals=2)
    dataset['longitude'] = numpy.around(dataset['longitude'].values, decimals=2)
    return dataset


def get_options():
    """
    Gets command line arguments and returns them.
    Options are accessed via options.ndvi, options.output, etc.

    Required arguments: ndvi, ndvi_var, spi, spi_var, et, et_var, sm, sm_var, output
    Optional arguments: verbose (v), multiprocessing, scales, weights

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
        '--weights',
        help='Path for the file containing custom weightings for the weighted average performed on input datasets.',
        default=None
    )
    parser.add_argument(
        '--output',
        help='Where to save the output file.',
        required=True
    )
    parser.add_argument(
        '-v', '--verbose',
        help='Increase output verbosity',
        action='store_const',
        const=logging.INFO,
        default=logging.WARN
    )
    parser.add_argument(
        '--multiprocessing',
        help='Number of processes to use in multiprocessing. Options: single, all_but_one, all. Defaults to '
             'all_but_one.',
        choices=["single", "all_but_one", "all"],
        required=False,
        default="all_but_one",
    )
    args = parser.parse_args()
    return args


def calc_cdi(dataset: xarray.Dataset, options):
    """
    Calculates the CDI based on the dataset containing all four variables. The CDI is a weighted average of these
    variables. The CDI is also percentile ranked at the end to ensure an even distribution.

    Will attempt to use custom weights for the input datasets obtained with a PCA analysis from --weights argument.
    If you don't have this file, it will use the default weights at the beginning of
    this file.

    :param dataset: Dataset containing soil moisture, ndvi, evapotranspiration and spi
    :param options: Object containing command-line options, used to get the name of the input variables
    :return: Dataset containing only the calculated CDI
    """
    if options.weights is not None:
        with xarray.open_dataset(options.weights) as weights:
            weights = weights.sel(latitude=dataset.latitude, longitude=dataset.longitude, method='nearest',
                                  tolerance=0.01).reindex_like(dataset, method='nearest', tolerance=0.01)
            dataset['cdi'] = dataset.groupby('time.month').apply(calc_cdi_for_month, args=(weights, options))
            dataset = dataset.drop('month', errors='ignore')
    else:
        LOGGER.warning('--weights argument not provided. Substituting approximate values. If this is not your '
                       'intention, please provide a custom weights file.')
        dataset['cdi'] = NDVI_WEIGHT * dataset[options.ndvi_var] \
                         + SPI_WEIGHT * dataset[options.spi_var] \
                         + ET_WEIGHT * (1 - dataset[options.et_var]) \
                         + SM_WEIGHT * dataset[options.sm_var]
    keys = dataset.keys()
    # Drop all input variables and anything else that slipped in, we ONLY want the CDI.
    for key in keys:
        if key != 'cdi':
            dataset = dataset.drop(key)
    dataset = dataset.dropna('time', how='all')
    # Percentile rank after finished.
    attrs = {var: dataset[var].attrs for var in set(dataset.keys()).union(set(dataset.dims))}
    stacked = dataset.cdi.stack(x=['latitude', 'longitude', 'time'])
    stacked = stacked.compute()
    stacked = stacked.rank(dim='x', pct=True, keep_attrs=False)
    dataset['cdi'] = stacked.unstack(dim='x')
    for key in attrs:
        dataset[key].attrs = attrs[key]
    dataset['latitude'].attrs['units'] = 'degrees_north'
    dataset['longitude'].attrs['units'] = 'degrees_east'
    utils.save_to_netcdf(dataset, options.output)
    return


def calc_cdi_for_month(dataset: xarray.Dataset, weights, options):
    """
    Designed to be used with a groupby().apply() operation.
    Calculates the CDI for one month of the year (January, February, etc.), since the weights applied to the input
    datasets are different for each month.
    """
    month = dataset.time.values[0].astype('<M8[M]').item().month
    weights = weights.sel(month=month)
    dataset['cdi'] = dataset[options.ndvi_var] * weights.ndvi \
                     + dataset[options.spi_var] * weights.spi \
                     + (1 - dataset[options.et_var]) * weights.et \
                     + dataset[options.sm_var] * weights.sm
    dataset = dataset.drop('month', errors='ignore')
    return dataset['cdi']


if __name__ == '__main__':
    main()
