import os
from dask.diagnostics import ProgressBar
from utils.logger_writer import LoggerWriter
import logging

"""
Saves an xarray Dataset to a netCDF file, with a progress bar (really common use case in this package).
Can log to a given logger and logging level. If these are not provided, will log on level WARN
"""


def save_to_netcdf(dataset, path, encoding=None, logging_level=logging.WARN):
    logging.basicConfig(level=logging.WARN, format="%(asctime)s %(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d  %H:%M:%S")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging_level)
    logger.info('Saving ' + path)
    try:
        if len(os.path.dirname(path)) > 0:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        if encoding is None:
            encoding = {}
        for key in dataset.keys():
            encoding[key] = {'zlib': True}
        delayed_obj = dataset.to_netcdf(path, compute=False, format='NETCDF4', engine='netcdf4', unlimited_dims='time',
                                        encoding=encoding)
        # Write this to log instead of stdout
        logger_writer = LoggerWriter(logger, logging.INFO)
        with ProgressBar(out=logger_writer, dt=1):
            delayed_obj.compute()
    except Exception as e:
        logger.error(e)
