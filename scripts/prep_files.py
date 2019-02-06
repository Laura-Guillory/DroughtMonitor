import xarray
from dask.diagnostics import ProgressBar
import argparse

DATASET_CHOICES = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                   'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp', 'monthly_rain',
                   'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit', 'avg_temp']
MERGED_FILE_PATH = 'data/{dataset}/merged_{dataset}.nc'
GLOB_FILE_PATH = 'data/{dataset}/*.{dataset}.nc'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-datasets',
        help='Choose which datasets to merge.',
        choices=DATASET_CHOICES + ['all'],
        nargs='*',
        required=True
    )
    args = parser.parse_args()
    if args.datasets == 'all':
        chosen_datasets = DATASET_CHOICES
    else:
        chosen_datasets = args.datasets

    for dataset_name in chosen_datasets:
        if dataset_name == 'avg_temp':
            continue
        else:
            merge_years(dataset_name)
    if 'avg_temp' in chosen_datasets:
        calc_avg_temp()


def calc_avg_temp():
    input_datasets = ['max_temp', 'min_temp']
    files = [
        MERGED_FILE_PATH.format(dataset=input_datasets[0]),
        MERGED_FILE_PATH.format(dataset=input_datasets[1])
    ]
    with xarray.open_mfdataset(files, chunks={'time': 10}) as dataset:
        avg_temp = xarray.DataArray(dataset.max_temp - dataset.min_temp)
        print(avg_temp)


def merge_years(dataset_name):
    inputs_path = GLOB_FILE_PATH.format(dataset=dataset_name)
    output_path = MERGED_FILE_PATH.format(dataset=dataset_name)
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
