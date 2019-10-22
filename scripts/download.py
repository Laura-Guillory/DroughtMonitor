import urllib.request
from urllib.error import HTTPError, ContentTooShortError
import os
from datetime import datetime
import argparse
import calendar
import logging

logging.basicConfig(level=logging.WARN, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d  %H:%M:%S")
LOGGER = logging.getLogger(__name__)


DOWNLOAD_URLS = {'SILO': 'https://s3-ap-southeast-2.amazonaws.com/silo-open-data/annual/{dataset}/{year}.{dataset}.nc',
                 'NDVI': 'http://www.bom.gov.au/web03/ncc/www/awap/ndvi/ndviave/month/grid/history/nat/{date_range}.Z',
                 'soil_moisture': 'http://www.bom.gov.au/jsp/awra/thredds/fileServer/AWRACMS/values/month/sm_pct.nc'}
DEFAULT_PATH = 'data/{dataset}/{date}.{dataset}.{filetype}'
DATASET_CHOICES = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                   'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp', 'monthly_rain',
                   'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit', 'ndvi', 'soil_moisture']


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
    for dataset in chosen_datasets:
        LOGGER.info('Downloading {} dataset.'.format(dataset))
        current_year = datetime.now().year
        if dataset == 'ndvi':
            for year in range(1992, current_year + 1):
                for month in range(1, 13):
                    date = '{y}-{m:02d}'.format(y=year, m=month)
                    day_of_year = calendar.monthrange(year, month)[1]
                    date_range = '{y}{m:02d}01{y}{m:02d}{d}'.format(y=year, m=month, d=day_of_year)
                    destination = path.format(dataset=dataset, date=date, filetype='txt.Z')
                    url = DOWNLOAD_URLS['NDVI'].format(date_range=date_range)
                    # Always redownload most recent year
                    if file_already_downloaded(destination) and year != datetime.now().year:
                        LOGGER.info('Already have {}'.format(url))
                        continue
                    try_to_download(url, destination)
        elif dataset == 'soil_moisture':
            destination = path.format(dataset=dataset, date='recent', filetype='nc')
            try_to_download(DOWNLOAD_URLS['soil_moisture'], destination)
        else:
            for year in range(1889, current_year + 1):
                destination = path.format(dataset=dataset, date=year, filetype='nc')
                url = DOWNLOAD_URLS['SILO'].format(dataset=dataset, year=year)
                # Always redownload most recent year
                if file_already_downloaded(destination) and year != datetime.now().year:
                    LOGGER.info('Already have {}'.format(url))
                    continue
                try_to_download(url, destination)
    LOGGER.info('Done!')


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


def try_to_download(url, destination):
    destination_dir = os.path.dirname(destination)
    os.makedirs(destination_dir, exist_ok=True)
    LOGGER.info('Downloading {} ...'.format(url))
    remaining_download_tries = 2
    opener = urllib.request.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    urllib.request.install_opener(opener)
    while remaining_download_tries > 0:
        try:
            urllib.request.urlretrieve(url, destination)
            return
        except (HTTPError, ValueError) as e:
            LOGGER.info('URL does not exist: ' + url)
            return
        except (TimeoutError, ConnectionResetError) as e:
            LOGGER.warning(e)
            remaining_download_tries -= 1
            continue
        except ContentTooShortError:
            remaining_download_tries -= 1
            continue
    LOGGER.warning('Download failed.')


def file_already_downloaded(path):
    if os.path.isfile(path):
        return True
    return False


if __name__ == '__main__':
    main()
