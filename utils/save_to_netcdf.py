import os
from dask.diagnostics import ProgressBar


def save_to_netcdf(dataset, file_path, encoding=None):
    print('Saving: ' + file_path)
    if len(os.path.dirname(file_path)) > 0:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if not encoding:
        encoding = {}
    for key in dataset.keys():
        encoding[key] = {'zlib': True}
    delayed_obj = dataset.to_netcdf(file_path, compute=False, format='NETCDF4', engine='netcdf4',
                                    unlimited_dims='time', encoding=encoding)
    with ProgressBar():
        delayed_obj.compute()
