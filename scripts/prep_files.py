import xarray
from dask.diagnostics import ProgressBar
import argparse
import os
from datetime import datetime
from scripts.truncate_time_dim import truncate_time_dim

DOWNLOADED_DATASETS = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                       'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp',
                       'monthly_rain', 'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit']
COMPUTED_DATASETS = ['avg_temp', 'monthly_avg_temp', 'monthly_et_short_crop']
DEFAULT_PATH = 'data/{dataset}/{year}.{dataset}.nc'

file_path = ''


def main():
    start_time = datetime.now()
    print('Starting time: ' + str(start_time))
    options = get_options()
    for dataset_name in options.datasets:
        # Create folder for results
        os.makedirs(os.path.dirname(options.path.format(dataset=dataset_name, year='')), exist_ok=True)
        if dataset_name in COMPUTED_DATASETS:
            continue
        else:
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
    with xarray.open_mfdataset(files, chunks={'time': 10}) as dataset:
        dataset['avg_temp'] = (dataset.max_temp + dataset.min_temp) / 2
        dataset['avg_temp'].attrs['units'] = dataset.max_temp.units
        dataset = dataset.drop('max_temp').drop('min_temp')
        # Dimensions must be in this order to be accepted by the climate indices tool
        dataset = dataset.transpose('lat', 'lon', 'time')
        output_file_path = get_merged_dataset_path(file_path, 'avg_temp')
        save_to_disk(dataset, output_file_path)


def calc_monthly_avg_temp(file_path):
    avg_temp_file_path = get_merged_dataset_path(file_path, 'avg_temp')
    if not os.path.isfile(avg_temp_file_path):
        calc_avg_temp(file_path)  # Calculate daily first, then load in to average over months
    with xarray.open_dataset(avg_temp_file_path) as dataset:
        monthly_avg = dataset.resample(time='M').mean().transpose('lat', 'lon', 'time')
        monthly_avg = truncate_time_dim(monthly_avg)
        monthly_avg['avg_temp'].attrs['units'] = dataset.avg_temp.units
        output_file_path = get_merged_dataset_path(file_path, 'monthly_avg_temp')
        save_to_disk(monthly_avg, output_file_path)


def calc_monthly_et_short_crop(file_path):
    et_short_crop_file_path = get_merged_dataset_path(file_path, 'et_short_crop')
    if not os.path.isfile(et_short_crop_file_path):
        merge_years('et_short_crop', file_path)
    with xarray.open_dataset(et_short_crop_file_path) as dataset:
        monthly_et = dataset.resample(time='M').sum(skipna=False).transpose('lat', 'lon', 'time')
        monthly_et = truncate_time_dim(monthly_et)
        monthly_et['et_short_crop'].attrs['units'] = dataset.et_short_crop.units
        output_file_path = get_merged_dataset_path(file_path, 'monthly_et_short_crop')
        save_to_disk(monthly_et, output_file_path)


def merge_years(dataset_name, file_path):
    output_path = get_merged_dataset_path(file_path, dataset_name)
    inputs_path = file_path.format(dataset=dataset_name, year='*')
    with xarray.open_mfdataset(inputs_path) as dataset:
        # Dimensions must be in this order to be accepted by the climate indices tool
        dataset = dataset.drop('crs').transpose('lat', 'lon', 'time')
        save_to_disk(dataset, output_path, encoding={'time': {'units': 'days since 1889-01', 'dtype': 'int64'}})


def save_to_disk(dataset, file_path, encoding=None):
    print('Saving: ' + file_path)
    if not encoding:
        encoding = {}
    for key in dataset.keys():
        encoding[key] = {'zlib': True}
    delayed_obj = dataset.to_netcdf(file_path, compute=False, format='NETCDF4', engine='netcdf4',
                                    unlimited_dims='time', encoding=encoding)
    with ProgressBar():
        delayed_obj.compute()


def get_merged_dataset_path(file_path, dataset_name):
    return os.path.dirname(file_path.format(dataset=dataset_name, year='')) + '/full_' + dataset_name + '.nc'


if __name__ == '__main__':
    main()
