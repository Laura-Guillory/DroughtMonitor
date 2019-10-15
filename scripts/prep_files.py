import xarray
import argparse
import os
from datetime import datetime
from scripts import truncate_time_dim
import utils
import glob
import subprocess
import logging
import warnings

logging.basicConfig(level=logging.WARN, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d  %H:%M:%S")
LOGGER = logging.getLogger(__name__)

DOWNLOADED_DATASETS = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                       'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp',
                       'monthly_rain', 'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit', 'ndvi']
ASCII_DATASETS = ['ndvi']
COMPUTED_DATASETS = ['monthly_avg_temp', 'monthly_et_short_crop']
DEFAULT_PATH = 'data/{dataset}/{year}.{dataset}.{filetype}'


def main():
    start_time = datetime.now()
    LOGGER.info('Starting time: ' + str(start_time))
    options = get_options()
    LOGGER.setLevel(options.verbose)
    for dataset_name in options.datasets:
        # Create folder for results
        os.makedirs(os.path.dirname(options.path.format(dataset=dataset_name, year='', filetype='-')), exist_ok=True)
        if dataset_name in ASCII_DATASETS:
            ascii_2_netcdf(dataset_name, options.path, logging_level=options.verbose)
        elif dataset_name not in COMPUTED_DATASETS:
            merge_years(dataset_name, options.path, logging_level=options.verbose)
    if 'monthly_avg_temp' in options.datasets:
        calc_monthly_avg_temp(options.path, logging_level=options.verbose)
    if 'monthly_et_short_crop' in options.datasets:
        calc_monthly_et_short_crop(options.path, logging_level=options.verbose)
    end_time = datetime.now()
    LOGGER.info('End time: ' + str(end_time))
    elapsed_time = end_time - start_time
    LOGGER.info('Elapsed time: ' + str(elapsed_time))


def get_options():
    """
    Gets command line arguments and returns them.
    Options are accessed via options.index_name, options.shape, etc.

    Required arguments: datasets
    Optional arguments: path

    Run this with the -h (help) argument for more detailed information. (python prep_files.py -h)

    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--datasets',
        help='Choose which datasets to prepare.',
        choices=DOWNLOADED_DATASETS + COMPUTED_DATASETS + ['all'],
        nargs='*',
        required=True
    )
    parser.add_argument(
        '--path',
        help='Determines where the input files can be found. Defaults to data/{dataset}/{year}.{dataset}.nc. Output '
             'will be saved in the same directory as \'full_{dataset}.nc\'',
        default=DEFAULT_PATH
    )
    parser.add_argument(
        '-v', '--verbose',
        help='Increase output verbosity',
        action='store_const',
        const=logging.INFO,
        default=logging.WARN
    )
    args = parser.parse_args()
    if args.datasets == 'all':
        args.datasets = DOWNLOADED_DATASETS + COMPUTED_DATASETS
    return args


def calc_monthly_avg_temp(file_path, logging_level=logging.INFO):
    # Too much data to merge min_temp and max_temp, then calculate average temperature for the whole thing
    # Convert all to monthly, then merge and calculate avg temperature
    input_datasets = ['max_temp', 'min_temp']
    for input_dataset in input_datasets:
        LOGGER.info('Calculating monthly ' + input_dataset)
        paths = glob.glob(file_path.format(dataset=input_dataset, year='*', filetype='nc'))
        for path in paths:
            with xarray.open_dataset(path) as dataset:
                # We expect warnings here about means of empty slices, just ignore them
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', category=RuntimeWarning)
                    monthly_dataset = dataset.resample(time='M').mean().transpose('lat', 'lon', 'time')
                monthly_dataset = truncate_time_dim(monthly_dataset)
                monthly_dataset[input_dataset].attrs['units'] = dataset[input_dataset].units
                output_file_path = path.replace(input_dataset, 'monthly_' + input_dataset)
                utils.save_to_netcdf(monthly_dataset, output_file_path, logging_level=logging.WARN)
        merge_years('monthly_' + input_dataset, file_path, logging_level=logging_level)
    LOGGER.info('Calculating monthly average temperature.')
    files = [get_merged_dataset_path(file_path, 'monthly_' + x) for x in input_datasets]
    with xarray.open_mfdataset(files, chunks={'time': 10}, combine='by_coords') as dataset:
        dataset['avg_temp'] = (dataset.max_temp + dataset.min_temp) / 2
        dataset['avg_temp'].attrs['units'] = dataset.max_temp.units
        # Dimensions must be in this order to be accepted by the climate indices tool
        dataset = dataset.drop('max_temp').drop('min_temp').transpose('lat', 'lon', 'time')
        output_file_path = get_merged_dataset_path(file_path, 'monthly_avg_temp')
        utils.save_to_netcdf(dataset, output_file_path, logging_level=logging_level)


def calc_monthly_et_short_crop(file_path, logging_level=logging.INFO):
    LOGGER.info('Calculating monthly short crop evapotranspiration.')
    # Too much data to merge et_short_crop and then calculate monthly.
    # So we calculate monthly for each individual file then merge
    et_paths = glob.glob(file_path.format(dataset='et_short_crop', year='*', filetype='nc'))
    for et_path in et_paths:
        with xarray.open_dataset(et_path) as dataset:
            monthly_et = dataset.resample(time='M').sum(skipna=False)
            monthly_et = truncate_time_dim(monthly_et)
            monthly_et['et_short_crop'].attrs['units'] = dataset.et_short_crop.units
            output_file_path = et_path.replace('et_short_crop', 'monthly_et_short_crop')
            utils.save_to_netcdf(monthly_et, output_file_path, logging_level=logging.WARN)
    merge_years('monthly_et_short_crop', file_path, logging_level=logging_level)


def merge_years(dataset_name, file_path, logging_level=logging.INFO):
    LOGGER.info('Merging files for: ' + dataset_name)
    output_path = get_merged_dataset_path(file_path, dataset_name)
    inputs_path = file_path.format(dataset=dataset_name, year='*', filetype='nc')
    with xarray.open_mfdataset(inputs_path, chunks={'time': 10}, combine='by_coords') as dataset:
        # Dimensions must be in this order to be accepted by the climate indices tool
        dataset = dataset.drop('crs', errors='ignore').transpose('lat', 'lon', 'time')
        encoding = {'time': {'units': 'days since 1889-01', '_FillValue': None}}
        utils.save_to_netcdf(dataset, output_path, encoding=encoding, logging_level=logging_level)


def get_merged_dataset_path(file_path, dataset_name):
    merged_file_path = file_path.format(dataset=dataset_name, year='', filetype='a')
    return os.path.dirname(merged_file_path) + '/full_' + dataset_name + '.nc'


def ascii_2_netcdf(dataset_name, file_path, logging_level=logging.INFO):
    LOGGER.info('Unpacking ASCII and merging files for: ' + dataset_name)
    # Unzip all files
    input_paths = glob.glob(file_path.format(dataset=dataset_name, year='*', filetype='txt.Z'))
    for path in input_paths:
        command = 'C:/Program Files/7-Zip/7z.exe' if os.name == 'nt' else '7z'
        subprocess.call([command, 'e', path, '-o' + os.path.dirname(path), '-y'], stdout=open(os.devnull, 'w'),
                        stderr=subprocess.STDOUT, close_fds=True)
    # Convert files to netcdf
    dates = []
    input_paths = glob.glob(file_path.format(dataset=dataset_name, year='*', filetype='txt'))
    for path in input_paths:
        file_name = path.split('\\')[-1]
        date = file_name.split('.')[0]
        datetime_object = datetime.strptime(date, '%Y-%m')
        dates.append(datetime_object)
    time_dim = xarray.Variable('time', dates)
    data_array = xarray.concat([xarray.open_rasterio(f) for f in input_paths], dim=time_dim)
    dataset = data_array.to_dataset(name=dataset_name).squeeze(drop=True).rename({'x': 'longitude', 'y': 'latitude'})
    dataset = dataset.where(dataset[dataset_name] != 99999.9)
    dataset['longitude'].attrs['units'] = 'degrees_east'
    dataset['latitude'].attrs['units'] = 'degrees_north'
    # Save as one file
    output_file_path = get_merged_dataset_path(file_path, dataset_name)
    utils.save_to_netcdf(dataset, output_file_path, logging_level=logging_level)


if __name__ == '__main__':
    main()
