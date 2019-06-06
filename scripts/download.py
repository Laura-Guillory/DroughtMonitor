from urllib.request import urlretrieve
from urllib.error import HTTPError, ContentTooShortError
import os
from datetime import datetime
import argparse

DOWNLOAD_URL = 'https://s3-ap-southeast-2.amazonaws.com/silo-open-data/annual/{dataset}/{year}.{dataset}.nc'
DEFAULT_PATH = 'data/{dataset}/{year}.{dataset}.nc'
DATASET_CHOICES = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                 'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp', 'monthly_rain',
                 'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit']


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
        help='Choose where to save the datasets.',
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
        for year in range(1889, current_year + 1):
            destination = path.format(dataset=dataset, year=year)
            url = DOWNLOAD_URL.format(dataset=dataset, year=year)
            if os.path.isfile(destination) and year != current_year:  # Always redownload most recent year
                print('Already have {}'.format(url))
                continue
            destination_dir = os.path.dirname(destination)
            os.makedirs(destination_dir, exist_ok=True)
            print('Downloading {} ...'.format(url))
            try_to_download(url, destination)
    print('Done!')


def try_to_download(url, destination):
    remaining_download_tries = 2
    while remaining_download_tries > 0:
        try:
            urlretrieve(url, destination)
            return
        except HTTPError:
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
