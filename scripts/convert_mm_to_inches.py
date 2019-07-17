import argparse
import xarray
from datetime import datetime
import os
from utils import save_to_netcdf

"""
Converts the units of a variable in a netCDF file from millimetres to inches. The variable name must be supplied.

To use from another Python script, just import convert_mm_to_inches() which returns an xarray Dataset.
"""

NUM_INCHES_IN_MM = 0.0393701


def main():
    start_time = datetime.now()
    print('Starting time: ' + str(start_time))

    options = get_options()
    dataset = xarray.open_dataset(options.input)
    result = convert_mm_to_inches(dataset, options.var)

    if options.output and options.output != options.input:
        save_to_netcdf(result, options.output)
    else:
        # xarray uses lazy loading from disk so overwriting the input file isn't possible without forcing a full load
        # into memory, which is infeasible with large datasets. Instead, save to a temp file, then remove the original
        # and rename the temp file to the original. As a bonus, this is atomic.
        temp_filename = options.input + '_temp'
        save_to_netcdf(result, temp_filename)
        dataset.close()
        os.remove(options.input)
        os.rename(temp_filename, options.input)

    end_time = datetime.now()
    print('End time: ' + str(end_time))
    elapsed_time = end_time - start_time
    print('Elapsed time: ' + str(elapsed_time))


def get_options():
    """
    Gets command line arguments and returns them.
    Options are accessed via options.input, options.output, etc.

    Required arguments: input, var
    Optional arguments: output

    Run this with the -h (help) argument for more detailed information. (python convert_mm_to_inches.py -h)

    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        help='The path of the file to use for this operation.',
        required=True
    )
    parser.add_argument(
        '--output',
        help='The location to save the result. If not supplied, the input file will be overwritten.'
    )
    parser.add_argument(
        '--var',
        help='The variable to convert from mm to inches.',
        required=True
    )
    args = parser.parse_args()
    return args


def convert_mm_to_inches(dataset, var):
    dataset[var].data = dataset[var].data * NUM_INCHES_IN_MM
    dataset[var].attrs['units'] = 'inches'
    dataset[var].attrs['min'] = 0
    return dataset


if __name__ == '__main__':
    main()
