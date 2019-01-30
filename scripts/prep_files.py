import xarray
from dask.diagnostics import ProgressBar
import argparse

dataset_paths = [{'inputs': 'data/daily_rain/*.daily_rain.nc', 'output': 'data/daily_rain/full_daily_rain.nc'}]
DATASET_CHOICES = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                   'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp', 'monthly_rain',
                   'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit']


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
        inputs_path = 'data/{dataset}/*.{dataset}.nc'.format(dataset=dataset_name)
        output_path = 'data/{dataset}/merged_{dataset}.nc'.format(dataset=dataset_name)
        with xarray.open_mfdataset(inputs_path) as dataset:
            dataset = dataset.transpose('lat', 'lon', 'time')
            encoding = {}
            for key in dataset.keys():
                encoding[key] = {'zlib': True}
            delayed_obj = dataset.to_netcdf(output_path, compute=False, format='netCDF4', engine='netcdf4',
                                            unlimited_dims='time', encoding=encoding)
            with ProgressBar():
                delayed_obj.compute()


if __name__ == '__main__':
    main()