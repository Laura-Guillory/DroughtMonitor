import argparse
import xarray
import netCDF4
from datetime import datetime
import os
import scipy.stats
import logging
import numpy
import utils
import shutil
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
    if options.output and options.output is not options.input:
        shutil.copyfile(options.input, options.output)
    else:
        options.output = options.input
    percentile_rank(options.output, options.vars, options.verbose)

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
        help='Which variables in the file to percentile rank. If not present, will rank all variables found.',
        nargs="+"
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


def percentile_rank(file_path, rank_vars=None, logging_level=logging.WARN):
    LOGGER.setLevel(logging_level)
    dataset = netCDF4.Dataset(file_path, mode='a')
    lon, lat = utils.get_lon_lat_names(dataset)
    num_months = len(dataset.dimensions['time'])
    num_rows = len(dataset.dimensions[lat])
    num_columns = len(dataset.dimensions[lon])
    et_dates = numpy.array(dataset.variables['time'])[:]

    for month_of_year in range(0, 12):
        month = month_of_year  # set i equal to m for counting
        num_years = int(num_months / 12)  # find number of years in the dataset
        if month_of_year < num_months % 12:
            num_years = num_years + 1
        month_et = numpy.full([num_years, num_rows, num_columns], numpy.nan)
        date_list = []
        i_list = []

        for year in range(0, num_years):
            try:
                month_et[year, :, :] = dataset.variables[rank_vars[0]][month, :, :]
            except ValueError:
                raise ValueError('Dimensions must be in order: time, latitude, longitude. Try using utils/transpose.py')
            date_list.append(et_dates[month])
            i_list.append(month)
            month = month + 12

        m_month_et = numpy.ma.masked_invalid(month_et)
        r_month_et = numpy.empty(m_month_et.shape)
        for row in range(0, num_rows):
            for column in range(0, num_columns):
                if m_month_et[:, row, column].count() == 0:
                    r_month_et[:, row, column] = 0
                else:
                    r_month_et[:, row, column] = scipy.stats.mstats.rankdata(m_month_et[:, row, column],
                                                                             use_missing=False)
        m_r_month_et = numpy.ma.array(r_month_et, mask=m_month_et.mask)
        pr_month_et = (m_r_month_et - 1) / m_r_month_et.count(axis=0)
        m_pr_month_et = numpy.ma.array(pr_month_et, mask=m_month_et.mask)

        for ix, ixf in enumerate(i_list):
            dataset['time'][ixf] = date_list[ix]
            dataset[rank_vars[0]][ixf] = m_pr_month_et[ix]
    dataset.close()


if __name__ == '__main__':
    main()
