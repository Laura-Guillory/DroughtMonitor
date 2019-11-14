import argparse
import xarray
from datetime import datetime
import utils
import logging
from percentile_rank import percentile_rank
import os
import multiprocessing
import warnings

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

NDVI_WEIGHT = 0.11792997
SPI_WEIGHT = 0.3153617
ET_WEIGHT = 0.25709686
SM_WEIGHT = 0.30961144


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
        result = calc_cdi(combined_dataset, options)
        cdi_temp_path = options.output_file_base + '1.nc.temp'
        utils.save_to_netcdf(result, cdi_temp_path, logging_level=options.verbose)
        for scale in options.scales:
            if scale == 1:
                continue
            else:
                output_path = options.output_file_base + str(scale) + '.nc'
                calc_averaged_cdi(cdi_temp_path, scale, output_path, options.verbose)

    # Remove 1 month CDI if it wasn't wanted
    if 1 not in options.scales:
        os.remove(cdi_temp_path)
    else:
        cdi_path = options.output_file_base + '1.nc'
        if os.path.exists(cdi_path):
            os.remove(cdi_path)
        os.rename(cdi_temp_path, cdi_path)

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
    with xarray.open_dataset(input_path) as dataset:
        ranked_dataset = percentile_rank(dataset, logging_level=verbosity, rank_vars=[var])
        ranked_dataset = utils.truncate_time_dim(ranked_dataset)
        utils.save_to_netcdf(ranked_dataset, output_path, logging_level=verbosity)


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
    dataset['latitude'] = dataset['latitude'].astype('f4')  # This fixes xarray dropping values to Nan in a stripe
    dataset['longitude'] = dataset['longitude'].astype('f4')  # pattern. The exact cause was never found.
    return dataset


def get_options():
    """
    Gets command line arguments and returns them.
    Options are accessed via options.ndvi, options.output, etc.

    Required arguments: ndvi, ndvi_var, spi, spi_var, et, et_var, sm, sm_var, output_file_base
    Optional arguments: verbose (v), multiprocessing, scales

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
        '--output_file_base',
        help='Base file name for all output files. Each computed scale for the CDI will have a corresponding output '
             'file that begins with thise base name plus a month scale.',
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
    parser.add_argument(
        '--scales',
        help='The number of months to average the results over. Multiple values can be given.',
        default=[1],
        type=int,
        nargs="+"
    )
    args = parser.parse_args()
    return args


def calc_cdi(dataset: xarray.Dataset, options):
    """
    Calculates the CDI based on the dataset containing all four variables.

    Will attempt to use custom weights for the input datasets obtained with a PCA analysis from
    inputdata_weights/weights.nc. If you don't have this file, it will use the default weights at the beginning of
    this file.

    :param dataset: Dataset containing soil moisture, ndvi, evapotranspiration and spi
    :param options: Object containing command-line options, used to get the name of the input variables
    :return: Dataset containing only the calculated CDI
    """
    try:
        with xarray.open_dataset('inputdata_weights/weights.nc') as weights:
            weights = weights.sel(latitude=dataset.latitude, longitude=dataset.longitude, method='nearest',
                                  tolerance=0.01).reindex_like(dataset, method='nearest', tolerance=0.01)
            dataset['cdi'] = dataset.groupby('time.month').apply(calc_cdi_for_month, args=(weights, options))
            dataset = dataset.drop('month', errors='ignore')
    except FileNotFoundError:
        LOGGER.warning('File for custom weightings not found at inputdata_weights/weights.nc. Substituting approximate '
                       'values. If this is not your intention, please obtain the weights.nc file.')
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
    # For some reason latitude becomes a double while longitude remains a float... tidy that up.
    dataset['latitude'] = dataset['latitude'].astype('f4')
    dataset['latitude'].attrs['units'] = 'degrees_north'
    dataset['longitude'].attrs['units'] = 'degrees_east'
    return dataset


def calc_cdi_for_month(dataset: xarray.Dataset, weights, options):
    """
    Designed to be used with a groupby().apply() operation.
    Calculates the CDI for one month of the year (January, February, etc.), since the weights applied to the input
    datasets are different for each month.
    """
    month = dataset.time.values[0].astype('<M8[M]').item().month
    ndvi_weight = weights.sel(dataset='ndvi', month=month).mean()
    spi_weight = weights.sel(dataset='spi', month=month).mean()
    et_weight = weights.sel(dataset='et', month=month).mean()
    sm_weight = weights.sel(dataset='sm', month=month).mean()
    dataset['cdi'] = dataset[options.ndvi_var] * ndvi_weight.weight \
                     + dataset[options.spi_var] * spi_weight.weight \
                     + (1 - dataset[options.et_var]) * et_weight.weight \
                     + dataset[options.sm_var] * sm_weight.weight
    dataset = dataset.drop('month', errors='ignore')
    return dataset['cdi']


def calc_averaged_cdi(cdi_path, scale, output_path, logging_level):
    """
    Uses the CDI to calculate averages over a defined number of months (scale)
    """
    # We expect warnings here about means of empty slices, just ignore them
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=RuntimeWarning)
        LOGGER.info('Calculating average over ' + str(scale) + ' months.')
        with xarray.open_dataset(cdi_path) as dataset:
            dataset = dataset.chunk({'time': 24})
            new_var_name = 'cdi_' + str(scale)
            dataset[new_var_name] = dataset['cdi'].rolling(time=scale).construct('window').mean('window')
            # This operation doesn't account for missing time entries. We need to remove results around those time gaps
            # that shouldn't have enough data to exist.
            time = dataset['time'].values
            dates_to_remove = []
            for i in range(len(time)):
                if i < scale:
                    continue
                # Get slice of dates for the size of the window
                window_dates = time[i-scale+1:i+1]
                first_date = window_dates[0].astype('<M8[M]').item()
                last_date = window_dates[-1].astype('<M8[M]').item()
                if ((last_date.year - first_date.year) * 12 + last_date.month - first_date.month) > scale:
                    dates_to_remove.append(time[i])
            dataset = dataset.drop(time=dates_to_remove)
            # Drop all input variables and anything else that slipped in, we ONLY want the new CDI.
            keys = dataset.keys()
            for key in keys:
                if key != new_var_name:
                    dataset = dataset.drop(key)
            dataset = dataset.dropna('time', how='all')
            utils.save_to_netcdf(dataset, output_path, logging_level=logging_level)


if __name__ == '__main__':
    main()
