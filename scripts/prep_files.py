import xarray
from dask.diagnostics import ProgressBar
import argparse

DATASET_CHOICES = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                   'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp', 'monthly_rain',
                   'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit', 'avg_temp']
DEFAULT_FILE_PATH = 'data'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-datasets',
        help='Choose which datasets to merge.',
        choices=DATASET_CHOICES + ['all'],
        nargs='*',
        required=True
    )
    parser.add_argument(
        '-path',
        help='The directory containing the dataset\'s directory.'
    )
    args = parser.parse_args()
    if args.datasets == 'all':
        chosen_datasets = DATASET_CHOICES
    else:
        chosen_datasets = args.datasets
    file_path = args.path if args.path else DEFAULT_FILE_PATH
    for dataset_name in chosen_datasets:
        if dataset_name == 'avg_temp':
            continue
        else:
            merge_years(dataset_name, file_path)
    if 'avg_temp' in chosen_datasets:
        calc_avg_temp(file_path)


def calc_avg_temp(file_path):
    input_datasets = ['max_temp', 'min_temp']
    merged_file_path = file_path + '/{dataset}/merged_{dataset}.nc'
    files = [
        merged_file_path.format(dataset=input_datasets[0]),
        merged_file_path.format(dataset=input_datasets[1])
    ]
    with xarray.open_mfdataset(files, chunks={'time': 10}) as dataset:
        avg_temp = xarray.DataArray((dataset.max_temp + dataset.min_temp) / 2)
        avg_temp.attrs['units'] = dataset.max_temp.units
        dataset['avg_temp'] = avg_temp
        dataset = dataset.drop('max_temp').drop('min_temp')
        dataset = dataset.transpose('lat', 'lon', 'time')
        encoding = {}
        for key in dataset.keys():
            encoding[key] = {'zlib': True}
        delayed_obj = dataset.to_netcdf(merged_file_path.format(dataset='avg_temp'), compute=False, format='NETCDF4',
                                        engine='netcdf4', unlimited_dims='time', encoding=encoding)
        with ProgressBar():
            delayed_obj.compute()


def merge_years(dataset_name, file_path):
    merged_file_path = file_path + '/{dataset}/merged_{dataset}.nc'
    glob_file_path = file_path + '/{dataset}/*.{dataset}.nc'
    inputs_path = glob_file_path.format(dataset=dataset_name)
    output_path = merged_file_path.format(dataset=dataset_name)
    with xarray.open_mfdataset(inputs_path) as dataset:
        dataset = dataset.transpose('lat', 'lon', 'time')
        encoding = {}
        for key in dataset.keys():
            encoding[key] = {'zlib': True}
        delayed_obj = dataset.to_netcdf(output_path, compute=False, format='NETCDF4', engine='netcdf4',
                                        unlimited_dims='time', encoding=encoding)
        with ProgressBar():
            delayed_obj.compute()


if __name__ == '__main__':
    main()
