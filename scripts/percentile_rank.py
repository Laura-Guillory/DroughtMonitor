import argparse
import xarray
from datetime import datetime
import os
import scipy.stats
import logging
import numpy
import utils
logging.basicConfig(level=logging.WARN, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d  %H:%M:%S")
LOGGER = logging.getLogger(__name__)

"""
Percentile ranks data for a netCDF file. Designed for climate data with time, longitude and latitude dimensions (in no
particular order). Each point on the grid is percentile ranked relative to the historical conditions on that month 
(e.g. 2001 January is ranked against all other Januaries). Will percentile rank all variables in the file by default, 
or this can be specified manually using --vars.
"""


# Raised when opening a netCDF with no variables to rank
class NoDataException(Exception):
    pass


def main():
    options = get_options()
    LOGGER.setLevel(options.verbose)
    start_time = datetime.now()
    LOGGER.info('Starting time: ' + str(start_time))
    dataset = xarray.open_dataset(options.input)
    result = percentile_rank(dataset, options.vars, options.verbose)

    if options.output and options.output is not options.input:
        utils.save_to_netcdf(result, options.output, logging_level=options.verbose)
    else:
        # xarray uses lazy loading from disk so overwriting the input file isn't possible without forcing a full load
        # into memory, which is infeasible with large datasets. Instead, save to a temp file, then remove the original
        # and rename the temp file to the original.
        temp_filename = options.output + '_temp'
        utils.save_to_netcdf(result, temp_filename, logging_level=options.verbose)
        dataset.close()
        os.remove(options.input)
        os.rename(temp_filename, options.output)

    end_time = datetime.now()
    LOGGER.info('End time: ' + str(end_time))
    elapsed_time = end_time - start_time
    LOGGER.info('Elapsed time: ' + str(elapsed_time))


def get_options():
    """
    Gets command line arguments and returns them.
    Options are accessed via options.input, options.output, etc.

    Required arguments: input
    Optional arguments: output, vars, verbose (v)

    Run this with the -h (help) argument for more detailed information. (python percentile_rank.py -h)

    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        help='The path of the file to use as input.',
        required=True
    )
    parser.add_argument(
        '--output',
        help='The location to save the result. If not supplied, the input file will be overwritten.'
    )
    parser.add_argument(
        '--vars',
        help='Which variables in the file to percentile rank. If not present, will rank all variables found.'
    )
    parser.add_argument(
        '-v', '--verbose',
        help='Increase output verbosity',
        action='store_const',
        const=logging.INFO,
        default=logging.WARN
    )
    args = parser.parse_args()
    if not args.output:
        args.output = args.input
    return args


def percentile_rank(dataset, rank_vars=None, logging_level=logging.WARN):
    """
    Percentile ranks variables in the given dataset. If options.var is defined, will rank only those variables.
    Otherwise, will rank all variables found in the dataset.

    :param dataset: The dataset to percentile rank
    :param rank_vars: Which variables in this dataset to rank
    :param logging_level: Set the logging level for this function, defaults to WARN
    :return: The percentile ranked dataset
    """
    LOGGER.setLevel(logging_level)
    # Get names of lon and lat dimensions
    lon, lat = utils.get_lon_lat_names(dataset)
    # Rechunk the data into chunks that will be fastest for this operation.
    dataset.chunk({lon: 20, lat: 20})
    # Get which variables will be ranked
    # If no options set, all variables will be ranked
    if rank_vars is not None:
        vars_to_rank = rank_vars
    else:
        vars_to_rank = list(dataset.keys())
        if len(vars_to_rank) == 0:
            raise NoDataException('This file does not contain any rankable variables.')

    # Groups lon and lat into 'loc' to apply the calculation to each unique coordinate
    # Using groupby.apply() on a stacked object loses all its attributes, save them to reapply after
    attrs = {var: dataset[var].attrs for var in set(dataset.keys()).union(set(dataset.dims))}
    dataset = dataset.stack(loc=[lat, lon])
    for var in vars_to_rank:
        # Each January is compared against the Januaries of all the other years to find its percentile rank.
        # Iterates through each month of the year to work on them one at a time.
        LOGGER.info('Calculating percentiles for variable ' + var.upper())
        dataset[var] = dataset[var].groupby('time.month').apply(calc_percentiles_for_month)
    dataset = dataset.unstack('loc')
    for key in attrs:
        dataset[key].attrs = attrs[key]
    # Set units for percentile ranked variables
    for var in vars_to_rank:
        dataset[var].attrs['units'] = 'percentile rank'
    # This gets automatically added to do the groupby() operation but is not needed for the result
    dataset = dataset.drop('month')
    # Drop unnecessary empty time slices
    dataset = dataset.dropna('time', how='all')
    return dataset


def calc_percentiles_for_month(dataarray):
    """
    Groups the data into each coordinate

    :param dataarray: A dataarray sliced to contain only one variable and one month of the year
    :return: The percentile ranked dataarray
    """
    return dataarray.groupby('loc').apply(calc_percentiles_for_coordinate)


def calc_percentiles_for_coordinate(dataarray):
    """
    Percentile ranks the data for a single coordinate

    :param dataarray: A dataarray sliced to contain only one variable, one month of the year, and one coordinate
    :return: The percentile ranked dataarray
    """
    # If this coordinate only has NaNs, there's nothing to rank
    if numpy.isnan(dataarray).all():
        return dataarray
    # Mask out any remaining NaNs
    masked_array = dataarray.to_masked_array()
    # Convert to percentile rank
    ranked_array = scipy.stats.mstats.rankdata(masked_array)
    # scipy.stats.mstats.rankdata() sets masked values to zero. Change them back to NaN.
    ranked_array[ranked_array == 0] = numpy.nan
    percentile_array = (ranked_array - 1) / len(dataarray)
    return percentile_array


if __name__ == '__main__':
    main()
