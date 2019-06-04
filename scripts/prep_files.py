import xarray
from dask.diagnostics import ProgressBar
import argparse
import os
from datetime import datetime
from scripts.truncate_time_dim import truncate_time_dim

DATASET_CHOICES = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                   'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp', 'monthly_rain',
                   'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit', 'avg_temp', 'avg_temp_monthly']
DEFAULT_FILE_PATH = 'data'


def main():
    start_time = datetime.now()
    print('Starting time: ' + str(start_time))
    options = get_options()
    for dataset_name in options.datasets:
        os.makedirs('{}/{}'.format(options.path, dataset_name), exist_ok=True)
        if dataset_name in ['avg_temp', 'avg_temp_monthly']:
            continue
        else:
            merge_years(dataset_name, options.path)
    if 'avg_temp' in options.datasets:
        calc_avg_temp(options.path)
    if 'avg_temp_monthly' in options.datasets:
        calc_avg_temp_monthly(options.path)
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
        choices=DATASET_CHOICES + ['all'],
        nargs='*',
        required=True
    )
    parser.add_argument(
        '--path',
        help='The directory containing the dataset\'s directory.',
        default=DEFAULT_FILE_PATH
    )
    args = parser.parse_args()
    if args.datasets == 'all':
        args.datasets = DATASET_CHOICES
    return args


def calc_avg_temp(file_path):
    input_datasets = ['max_temp', 'min_temp']
    merge_prerequisite_datasets(input_datasets, file_path)
    merged_file_path = file_path + '/{dataset}/merged_{dataset}.nc'
    files = [
        merged_file_path.format(dataset=input_datasets[0]),
        merged_file_path.format(dataset=input_datasets[1])
    ]
    with xarray.open_mfdataset(files, chunks={'time': 10}) as dataset:
        dataset['avg_temp'] = (dataset.max_temp + dataset.min_temp) / 2
        dataset['avg_temp'].attrs['units'] = dataset.max_temp.units
        dataset = dataset.drop('max_temp').drop('min_temp')
        # Dimensions must be in this order to be accepted by the climate indices tool
        dataset = dataset.transpose('lat', 'lon', 'time')
        encoding = {}
        for key in dataset.keys():
            encoding[key] = {'zlib': True}
        delayed_obj = dataset.to_netcdf(merged_file_path.format(dataset='avg_temp'), compute=False, format='NETCDF4',
                                        engine='netcdf4', unlimited_dims='time', encoding=encoding)
        with ProgressBar():
            delayed_obj.compute()


def merge_prerequisite_datasets(datasets, path):
    for dataset_name in datasets:
        if not os.path.isfile('{path}/{dataset}/merged_{dataset}.nc'.format(path=path, dataset=dataset_name)):
            merge_years(dataset_name, path)


def calc_avg_temp_monthly(file_path):
    avg_temp_file_path = file_path + '/avg_temp/merged_avg_temp.nc'
    if not os.path.isfile(avg_temp_file_path):
        calc_avg_temp(file_path)  # Calculate daily first, then load in to average over months
    with xarray.open_dataset(avg_temp_file_path) as dataset:
        monthly_avg = dataset.resample(time='M').mean().transpose('lat', 'lon', 'time')
        monthly_avg['avg_temp'].attrs['units'] = dataset.avg_temp.units
        encoding = {}
        for key in monthly_avg.keys():
            encoding[key] = {'zlib': True}
        output_file_path = file_path + '/avg_temp_monthly/merged_avg_temp_monthly.nc'
        delayed_obj = monthly_avg.to_netcdf(output_file_path.format(dataset='avg_temp'), compute=False,
                                            format='NETCDF4', engine='netcdf4', unlimited_dims='time',
                                            encoding=encoding)
        with ProgressBar():
            delayed_obj.compute()


def merge_years(dataset_name, file_path):
    merged_file_path = file_path + '/{dataset}/merged_{dataset}.nc'
    glob_file_path = file_path + '/{dataset}/*.{dataset}.nc'
    inputs_path = glob_file_path.format(dataset=dataset_name)
    output_path = merged_file_path.format(dataset=dataset_name)
    with xarray.open_mfdataset(inputs_path) as dataset:
        # Dimensions must be in this order to be accepted by the climate indices tool
        dataset = dataset.transpose('lat', 'lon', 'time')
        # Standardise to the 1st of every month or mismatched time entries will confused climate indices tool
        dataset = truncate_time_dim(dataset)
        encoding = {'time': {'units': 'days since 1889-01', 'dtype': 'int64'}}
        for key in dataset.keys():
                encoding[key] = {'zlib': True}
        for key in dataset.keys():
            encoding[key] = {'zlib': True}
        delayed_obj = dataset.to_netcdf(output_path, compute=False, format='NETCDF4', engine='netcdf4',
                                        unlimited_dims='time', encoding=encoding)
        with ProgressBar():
            delayed_obj.compute()


if __name__ == '__main__':
    main()
