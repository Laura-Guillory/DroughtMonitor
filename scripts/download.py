import urllib.request
from urllib.error import HTTPError, ContentTooShortError
import os
from datetime import datetime
import argparse
import calendar


DOWNLOAD_URLS = {'SILO': 'https://s3-ap-southeast-2.amazonaws.com/silo-open-data/annual/{dataset}/{year}.{dataset}.nc',
                 'NDVI': 'http://www.bom.gov.au/web03/ncc/www/awap/ndvi/ndviave/month/grid/history/nat/{date_range}.Z'}
DEFAULT_PATH = 'data/{dataset}/{date}.{dataset}.{filetype}'
DATASET_CHOICES = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                 'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp', 'monthly_rain',
                 'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit', 'ndvi']


def main():
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
    args = parser.parse_args()
    if len(args.datasets) == 1 and args.datasets[0] == 'all':
        chosen_datasets = DATASET_CHOICES
    else:
        chosen_datasets = args.datasets
    path = args.path if args.path else DEFAULT_PATH
    for dataset in chosen_datasets:
        print('Downloading {} dataset.'.format(dataset))
        current_year = datetime.now().year
        if dataset == 'ndvi':
            for year in range(1992, current_year + 1):
                for month in range(1, 13):
                    destination = path.format(dataset=dataset, date='{y}-{m:02d}'.format(y=year, m=month), filetype='Z')
                    date_range = '{y}{m:02d}01{y}{m:02d}{d}'.format(y=year, m=month, d=calendar.monthrange(year, month)[1])
                    url = DOWNLOAD_URLS['NDVI'].format(date_range=date_range)
                    try_to_download(url, destination, year)
        else:
            for year in range(1889, current_year + 1):
                destination = path.format(dataset=dataset, date=year, filetype='nc')
                url = DOWNLOAD_URLS['SILO'].format(dataset=dataset, year=year)
                try_to_download(url, destination, year)
    print('Done!')


def try_to_download(url, destination, year):
    if os.path.isfile(destination) and year != datetime.now().year:  # Always redownload most recent year
        print('Already have {}'.format(url))
        return
    destination_dir = os.path.dirname(destination)
    os.makedirs(destination_dir, exist_ok=True)
    print('Downloading {} ...'.format(url))
    remaining_download_tries = 2
    opener = urllib.request.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    urllib.request.install_opener(opener)
    while remaining_download_tries > 0:
        try:
            urllib.request.urlretrieve(url, destination, )
            return
        except (HTTPError, ValueError) as e:
            print('URL does not exist: ' + url)
            return
        except (TimeoutError, ConnectionResetError) as e:
            print(e)
            remaining_download_tries -= 1
            continue
        except ContentTooShortError:
            remaining_download_tries -= 1
            continue
    print('Download failed.')


if __name__ == '__main__':
    main()
