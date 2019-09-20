import os
from dask.diagnostics import ProgressBar
from utils.logger_writer import LoggerWriter
import logging

"""
Saves an xarray Dataset to a netCDF file, with a progress bar (really common use case in this package).
Can log to a given logger and logging level. If these are not provided, will provide its own logger set to logging level
WARN.
"""


class NetCDFSaver:
    def __init__(self, logger=None, level=logging.WARN):
        self.logger = logger if logger else logging.getLogger()
        self.level = level

    def save(self, dataset, file_path, encoding=None):
        self.logger.info('Saving ' + file_path)
        if len(os.path.dirname(file_path)) > 0:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
        if not encoding:
            encoding = {}
        for key in dataset.keys():
            encoding[key] = {'zlib': True}
        delayed_obj = dataset.to_netcdf(file_path, compute=False, format='NETCDF4', engine='netcdf4',
                                        unlimited_dims='time', encoding=encoding)
        # Write this to log instead of stdout
        logger_writer = LoggerWriter(self.logger, logging.INFO)
        with ProgressBar(out=logger_writer, dt=1):
            delayed_obj.compute()
