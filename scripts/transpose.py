import argparse
import xarray
from datetime import datetime
import os
from utils.netcdf_saver import NetCDFSaver

"""
Saves the dimensions of a netCDF file in a different order, because some programs will expect the dimensions to be
ordered a specific way and won't run without it.

To use from another Python script, just import transpose() which returns an xarray Dataset.
"""


def main():
    start_time = datetime.now()
    print('Starting time: ' + str(start_time))

    options = get_options()
    dataset = xarray.open_dataset(options.input)
    result = transpose(dataset, options.dims)

    if options.output and not options.output == options.input:
        NetCDFSaver().save(result, options.output)
    else:
        # xarray uses lazy loading from disk so overwriting the input file isn't possible without forcing a full load
        # into memory, which is infeasible with large datasets. Instead, save to a temp file, then remove the original
        # and rename the temp file to the original. As a bonus, this is atomic.
        temp_filename = options.input + '_temp'
        NetCDFSaver().save(result, temp_filename)
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

    Required arguments: input, dims
    Optional arguments: output

    Run this with the -h (help) argument for more detailed information. (python transpose.py -h)

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
        '--dims',
        help='The desired order of the dimensions.',
        nargs='+',
        required=True
    )
    args = parser.parse_args()
    return args


def transpose(dataset, dims):
    return dataset.transpose(*dims)


if __name__ == '__main__':
    main()
