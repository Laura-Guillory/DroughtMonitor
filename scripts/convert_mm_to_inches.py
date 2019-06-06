import argparse
import xarray
from datetime import datetime
import os

NUM_INCHES_IN_MM = 0.0393701


def main():
    start_time = datetime.now()
    print('Starting time: ' + str(start_time))

    options = get_options()
    dataset = xarray.open_dataset(options.input)
    result = convert_mm_to_inches(dataset, options.var)

    if options.output and options.output is not options.input:
        result.to_netcdf(options.output)
    else:
        # xarray uses lazy loading from disk so overwriting the input file isn't possible without forcing a full load
        # into memory, which is infeasible with large datasets. Instead, save to a temp file, then remove the original
        # and rename the temp file to the original. As a bonus, this is atomic.
        temp_filename = options.output + '_temp'
        result.to_netcdf(temp_filename)
        dataset.close()
        os.remove(options.input)
        os.rename(temp_filename, options.output)

    end_time = datetime.now()
    print('End time: ' + str(end_time))
    elapsed_time = end_time - start_time
    print('Elapsed time: ' + str(elapsed_time))


def get_options():
    """
    Gets command line arguments and returns them.
    Options are accessed via options.input, options.output, etc.

    Required arguments: input
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
    if not args.output:
        args.output = args.input
    return args


def convert_mm_to_inches(dataset, var):
    dataset[var].data = dataset[var].data * NUM_INCHES_IN_MM
    dataset[var].attrs['units'] = 'inches'
    dataset[var].attrs['min'] = 0
    return dataset


if __name__ == '__main__':
    main()
