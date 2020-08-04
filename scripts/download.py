import os
from datetime import datetime
import argparse
import calendar
import logging
import xarray
import numpy
import utils
import requests
from requests.auth import HTTPBasicAuth
from getpass import getpass

logging.basicConfig(level=logging.WARN, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d  %H:%M:%S")
LOGGER = logging.getLogger(__name__)


DOWNLOAD_URLS = {'SILO': 'https://s3-ap-southeast-2.amazonaws.com/silo-open-data/annual/{dataset}/{year}.{dataset}.nc',
                 'NDVI': {
                     'realtime': 'https://land.copernicus.vgt.vito.be/manifest/ndvi300_v1_333m/manifest_cgls_ndvi300_v1'
                                 '_333m_latest.txt',
                     'archive': 'https://land.copernicus.vgt.vito.be/manifest/ndvi_v2_1km/manifest_cgls_ndvi_v2_1km_lat'
                                'est.txt'},
                 'soil_moisture': 'http://www.bom.gov.au/jsp/awra/thredds/fileServer/AWRACMS/values/month/sm_pct.nc'}
DEFAULT_PATH = 'data/{dataset}/{date}.{dataset}.{filetype}'
DATASET_CHOICES = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                   'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp', 'monthly_rain',
                   'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit', 'ndvi', 'soil_moisture']
DAILY_DATASETS = ['daily_rain', 'et_short_crop', 'max_temp', 'min_temp']


def main():
    options = get_options()
    LOGGER.setLevel(options.verbose)
    start_time = datetime.now()
    LOGGER.info('Starting time: ' + str(start_time))
    if len(options.datasets) == 1 and options.datasets[0] == 'all':
        chosen_datasets = DATASET_CHOICES
    else:
        chosen_datasets = options.datasets
    path = options.path if options.path else DEFAULT_PATH
    download_datasets(path, chosen_datasets)
    check_data_is_current(path, chosen_datasets)


def download_datasets(path, datasets):
    for dataset in datasets:
        LOGGER.info('Downloading {} dataset.'.format(dataset))
        current_month = datetime.now().month - 1 if datetime.now().month > 1 else 12
        current_year = datetime.now().year if datetime.now().month > 1 else datetime.now().year - 1
        if dataset == 'ndvi':
            # Get archive
            # Get manifest file
            manifest_destination = os.path.dirname(path.format(dataset=dataset, date='', filetype='')) \
                                   + '/archive_manifest.txt'
            try_to_download(DOWNLOAD_URLS['NDVI']['archive'], manifest_destination)
            username = input('Username: ')
            password = getpass()
            # Download all files listed in manifest
            with open(manifest_destination) as manifest:
                for line in manifest:
                    parts = line.split('/')
                    year = parts[8]
                    month = parts[9]
                    day = parts[10]
                    date = '{y}-{m}-{d}'.format(y=year, m=month, d=day)
                    file_destination = path.format(dataset=dataset, date='1km.archive.' + date, filetype='nc')
                    # Always redownload most recent
                    if file_already_downloaded(file_destination) and (int(year) != current_year or int(month) != current_month):
                        continue
                    if int(year) == current_year and int(month) > current_month:  # The future
                        continue
                    try_to_download(line[:-1], file_destination, auth=(username, password))
            # Get realtime
            # Get manifest file
            manifest_destination = os.path.dirname(path.format(dataset=dataset, date='', filetype='')) \
                                   + '/realtime_manifest.txt'
            try_to_download(DOWNLOAD_URLS['NDVI']['realtime'], manifest_destination)
            # Download all realtime files that aren't in archive
            with open(manifest_destination) as manifest:
                for line in manifest:
                    parts = line.split('/')
                    year = parts[8]
                    month = parts[9]
                    day = parts[10]
                    date = '{y}-{m}-{d}'.format(y=year, m=month, d=day)
                    file_destination = path.format(dataset=dataset, date='300m.realtime.' + date, filetype='nc')
                    archive_file_destination = path.format(dataset=dataset, date='1km.archive.' + date, filetype='nc')
                    # Always redownload most recent
                    if file_already_downloaded(file_destination) and (int(year) != current_year or int(month) != current_month):
                        continue
                    if int(year) == current_year and int(month) > current_month:  # The future
                        continue
                    if file_already_downloaded(archive_file_destination):  # Files already in archive
                        continue
                    try_to_download(line[:-1], file_destination, auth=(username, password))
        elif dataset == 'soil_moisture':
            file_destination = path.format(dataset=dataset, date='realtime', filetype='nc')
            try_to_download(DOWNLOAD_URLS['soil_moisture'], file_destination)
        else:
            for year in range(1889, current_year + 1):
                file_destination = path.format(dataset=dataset, date=year, filetype='nc')
                url = DOWNLOAD_URLS['SILO'].format(dataset=dataset, year=year)
                if not file_already_downloaded(file_destination) or need_to_redownload_month(year, current_month):
                    try_to_download(url, file_destination)


def need_to_redownload_month(file_year, current_month):
    if file_year == datetime.now().year:
        return True
    if current_month == 12 and file_year == datetime.now().year-1:
        return True
    return False


def get_options():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--datasets',
        help='Choose which datasets to download.',
        choices=DATASET_CHOICES + ['all'],
        nargs='*',
        required=True
    )
    parser.add_argument(
        '--path',
        help='Choose where to save the datasets.'
    )
    parser.add_argument(
        '-v', '--verbose',
        help='Increase output verbosity',
        action='store_const',
        const=logging.INFO,
        default=logging.WARN
    )
    return parser.parse_args()


def try_to_download(url, destination, auth=None):
    destination_dir = os.path.dirname(destination)
    os.makedirs(destination_dir, exist_ok=True)
    LOGGER.info('Downloading {} ...'.format(url))
    remaining_download_tries = 2
    while remaining_download_tries > 0:
        try:
            if auth is not None:
                username, password = auth
                login = HTTPBasicAuth(username, password)
            else:
                login = None
            with requests.get(url, stream=True, auth=login) as r:
                r.raise_for_status()
                with open(destination, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return
        except Exception as e:
            LOGGER.info(repr(e))
            remaining_download_tries -= 1
            continue
    LOGGER.warning('Download failed.')


def file_already_downloaded(path):
    if os.path.isfile(path):
        return True
    return False


def check_data_is_current(path, dataset_names):
    date = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if date.month == 1:
        date = date.replace(year=date.year-1)
    date = date.replace(month=12 if date.month == 1 else date.month - 1)
    LOGGER.info(date.strftime('Checking if data has been released for %B %Y:'))
    for dataset_name in dataset_names:
        if dataset_name == 'ndvi':
            destination = path.format(dataset=dataset_name, date=date.strftime('%Y-%m'), filetype='txt.Z')
            if os.path.exists(destination):
                LOGGER.info(dataset_name + ": Yes")
            else:
                LOGGER.info(dataset_name + ": No")
        elif dataset_name == 'soil_moisture':
            destination = path.format(dataset=dataset_name, date='realtime', filetype='nc')
            fix_time_dimension(destination)
            with xarray.open_dataset(destination) as soil_moisture:
                date_modified_str = soil_moisture.attrs['date_modified']
                date_file_modified = datetime.strptime(date_modified_str, '%Y-%m-%dT%H:%M:%S')
                date_file_modified = date_file_modified.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                if date <= date_file_modified:
                    LOGGER.info(dataset_name + ": Yes")
                else:
                    LOGGER.info(dataset_name + ": No")
        else:
            destination = path.format(dataset=dataset_name, date=date.year, filetype='nc')
            with xarray.open_dataset(destination) as dataset:
                last_day_of_month = date.replace(day=calendar.monthrange(date.year, date.month)[1])
                if dataset_name in DAILY_DATASETS:
                    time_slice = slice(last_day_of_month.strftime('%Y-%m-%d'), last_day_of_month.strftime('%Y-%m-%d'))
                else:
                    time_slice = slice(date.strftime('%Y-%m-%d'), last_day_of_month.strftime('%Y-%m-%d'))
                subset = dataset.sel(time=time_slice)
                if subset['time'].values.size > 0 and not numpy.isnan(subset[dataset_name]).all():
                    LOGGER.info(dataset_name + ": Yes")
                else:
                    LOGGER.info(dataset_name + ": No")


def fix_time_dimension(dataset_path):
    # For whatever reason an extra variable gets inserted into these files that makes the time decoding utterly fail.
    # The soil moisture file can't be opened properly without doing this
    with xarray.open_dataset(dataset_path, chunks={'time': 10}, decode_times=False) as dataset:
        dataset = dataset.drop_vars('time_bounds', errors='ignore')
        utils.save_to_netcdf(dataset, dataset_path + '.temp')
    # Dates need to be truncated to each month and saved to file - if not saved to file the combine doesn't work.
    with xarray.open_dataset(dataset_path + '.temp', chunks={'time': 10}) as dataset:
        dataset = utils.truncate_time_dim(dataset)
        utils.save_to_netcdf(dataset, dataset_path)


if __name__ == '__main__':
    main()
