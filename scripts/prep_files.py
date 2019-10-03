import xarray
import argparse
import os
from datetime import datetime
from scripts.truncate_time_dim import truncate_time_dim
from utils import save_to_netcdf
import glob
import subprocess

DOWNLOADED_DATASETS = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                       'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp',
                       'monthly_rain', 'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit', 'ndvi']
ASCII_DATASETS = ['ndvi']
COMPUTED_DATASETS = ['avg_temp', 'monthly_avg_temp', 'monthly_et_short_crop']
DEFAULT_PATH = 'data/{dataset}/{year}.{dataset}.{filetype}'


def main():
    start_time = datetime.now()
    print('Starting time: ' + str(start_time))
    options = get_options()
    for dataset_name in options.datasets:
        # Create folder for results
        os.makedirs(os.path.dirname(options.path.format(dataset=dataset_name, year='', filetype='-')), exist_ok=True)
        if dataset_name in ASCII_DATASETS:
            ascii_2_netcdf(dataset_name, options.path)
        elif dataset_name not in COMPUTED_DATASETS:
            merge_years(dataset_name, options.path)
    if 'avg_temp' in options.datasets:
        calc_avg_temp(options.path)
    if 'monthly_avg_temp' in options.datasets:
        calc_monthly_avg_temp(options.path)
    if 'monthly_et_short_crop' in options.datasets:
        calc_monthly_et_short_crop(options.path)
    end_time = datetime.now()
    print('End time: ' + str(end_time))
    elapsed_time = end_time - start_time
    print('Elapsed time: ' + str(elapsed_time))


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
    args = parser.parse_args()
    if args.datasets == 'all':
        args.datasets = DOWNLOADED_DATASETS + COMPUTED_DATASETS
    return args


def calc_avg_temp(file_path):
    input_datasets = ['max_temp', 'min_temp']
    for input_dataset in input_datasets:
        if not os.path.isfile(get_merged_dataset_path(file_path, input_dataset)):
            merge_years(input_dataset, file_path)
    files = [get_merged_dataset_path(file_path, x) for x in input_datasets]
    with xarray.open_mfdataset(files, chunks={'time': 10}, combine='by_coords') as dataset:
        dataset['avg_temp'] = (dataset.max_temp + dataset.min_temp) / 2
        dataset['avg_temp'].attrs['units'] = dataset.max_temp.units
        dataset = dataset.drop('max_temp').drop('min_temp')
        # Dimensions must be in this order to be accepted by the climate indices tool
        dataset = dataset.transpose('lat', 'lon', 'time')
        output_file_path = get_merged_dataset_path(file_path, 'avg_temp')
        save_to_netcdf(dataset, output_file_path)


def calc_monthly_avg_temp(file_path):
    avg_temp_file_path = get_merged_dataset_path(file_path, 'avg_temp')
    if not os.path.isfile(avg_temp_file_path):
        calc_avg_temp(file_path)  # Calculate daily first, then load in to average over months
    with xarray.open_dataset(avg_temp_file_path) as dataset:
        monthly_avg = dataset.resample(time='M').mean().transpose('lat', 'lon', 'time')
        monthly_avg = truncate_time_dim(monthly_avg)
        monthly_avg['avg_temp'].attrs['units'] = dataset.avg_temp.units
        output_file_path = get_merged_dataset_path(file_path, 'monthly_avg_temp')
        save_to_netcdf(monthly_avg, output_file_path)


def calc_monthly_et_short_crop(file_path):
    et_short_crop_file_path = get_merged_dataset_path(file_path, 'et_short_crop')
    if not os.path.isfile(et_short_crop_file_path):
        merge_years('et_short_crop', file_path)
    with xarray.open_dataset(et_short_crop_file_path) as dataset:
        monthly_et = dataset.resample(time='M').sum(skipna=False).transpose('lat', 'lon', 'time')
        monthly_et = truncate_time_dim(monthly_et)
        monthly_et['et_short_crop'].attrs['units'] = dataset.et_short_crop.units
        output_file_path = get_merged_dataset_path(file_path, 'monthly_et_short_crop')
        save_to_netcdf(monthly_et, output_file_path)


def merge_years(dataset_name, file_path):
    output_path = get_merged_dataset_path(file_path, dataset_name)
    inputs_path = file_path.format(dataset=dataset_name, year='*', filetype='nc')
    with xarray.open_mfdataset(inputs_path, combine='by_coords') as dataset:
        # Dimensions must be in this order to be accepted by the climate indices tool
        dataset = dataset.drop('crs').transpose('lat', 'lon', 'time')
        encoding = {'time': {'units': 'days since 1889-01', '_FillValue': None}}
        save_to_netcdf(dataset, output_path, encoding=encoding)


def get_merged_dataset_path(file_path, dataset_name):
    merged_file_path = file_path.format(dataset=dataset_name, year='', filetype='a')
    return os.path.dirname(merged_file_path) + '/full_' + dataset_name + '.nc'


def ascii_2_netcdf(dataset_name, file_path):
    # Unzip all files
    input_paths = glob.glob(file_path.format(dataset=dataset_name, year='*', filetype='txt.Z'))
    for path in input_paths:
        with open(path, 'rb') as zipped_file:
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
    save_to_netcdf(dataset, output_file_path)


if __name__ == '__main__':
    main()
