import xarray
import argparse
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import utils
import glob
import logging
import warnings
import shutil
import calendar
import math
import numpy
from cartopy.io import shapereader
import regionmask

logging.basicConfig(level=logging.WARN, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d  %H:%M:%S")
LOGGER = logging.getLogger(__name__)

DOWNLOADED_DATASETS = ['daily_rain', 'et_morton_actual', 'et_morton_potential', 'et_morton_wet', 'et_short_crop',
                       'et_tall_crop', 'evap_morton_lake', 'evap_pan', 'evap_syn', 'max_temp', 'min_temp',
                       'monthly_rain', 'mslp', 'radiation', 'rh_tmax', 'vp', 'vp_deficit', 'ndvi', 'soil_moisture']
COMPUTED_DATASETS = ['monthly_avg_temp', 'monthly_et_short_crop']
CALC_MORE_TIME_PERIODS = ['monthly_et_short_crop', 'ndvi', 'soil_moisture']
OTHER_DATASETS = ['soil_moisture', 'ndvi']
DEFAULT_PATH = 'data/{dataset}/{year}.{dataset}.{filetype}'
DAILY_DATASETS = ['daily_rain', 'et_short_crop', 'max_temp', 'min_temp']


def main():
    start_time = datetime.now()
    LOGGER.info('Starting time: ' + str(start_time))
    options = get_options()
    LOGGER.setLevel(options.verbose)
    for dataset_name in options.datasets:
        # Create folder for results
        os.makedirs(os.path.dirname(options.path.format(dataset=dataset_name, year='', filetype='-')), exist_ok=True)
        if dataset_name not in COMPUTED_DATASETS + OTHER_DATASETS:
            merge_years(dataset_name, options.path)
    if 'monthly_avg_temp' in options.datasets:
        calc_monthly_avg_temp(options.path)
    if 'monthly_et_short_crop' in options.datasets:
        calc_monthly_et_short_crop(options.path)
    if 'soil_moisture' in options.datasets:
        combine_soil_moisture(options.path)
    if 'ndvi' in options.datasets:
        combine_ndvi(options.path)
    for dataset_name in options.datasets:
        if dataset_name in CALC_MORE_TIME_PERIODS:
            for scale in [3, 6, 9, 12, 24, 36]:
                avg_over_period(dataset_name, options.path, scale)
    end_time = datetime.now()
    LOGGER.info('End time: ' + str(end_time))
    elapsed_time = end_time - start_time
    LOGGER.info('Elapsed time: ' + str(elapsed_time))


def get_options():
    """
    Gets command line arguments and returns them.
    Options are accessed via options.index_name, options.shape, etc.

    Required arguments: datasets
    Optional arguments: path

    Run this with the -h (help) argument for more detailed information. (python prep_files.py -h)

    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--datasets',
        help='Choose which datasets to prepare.',
        choices=DOWNLOADED_DATASETS + COMPUTED_DATASETS + ['all'],
        nargs='*',
        required=True
    )
    parser.add_argument(
        '--path',
        help='Determines where the input files can be found. Defaults to data/{dataset}/{year}.{dataset}.nc. Output '
             'will be saved in the same directory as \'full_{dataset}.nc\'',
        default=DEFAULT_PATH
    )
    parser.add_argument(
        '-v', '--verbose',
        help='Increase output verbosity',
        action='store_const',
        const=logging.INFO,
        default=logging.WARN
    )
    args = parser.parse_args()
    if args.datasets == 'all':
        args.datasets = DOWNLOADED_DATASETS + COMPUTED_DATASETS
    return args


def calc_monthly_avg_temp(file_path):
    # Too much data to merge min_temp and max_temp, then calculate average temperature for the whole thing
    # Convert all to monthly, then merge and calculate avg temperature
    input_datasets = ['max_temp', 'min_temp']
    for input_dataset in input_datasets:
        LOGGER.info('Calculating monthly ' + input_dataset)
        paths = glob.glob(file_path.format(dataset=input_dataset, year='*', filetype='nc'))
        for path in paths:
            with xarray.open_dataset(path) as dataset:
                # If the last month is incomplete we should drop it
                dataset = drop_incomplete_months(dataset)

                # We expect warnings here about means of empty slices, just ignore them
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', category=RuntimeWarning)
                    monthly_dataset = dataset.resample(time='M').mean().transpose('lat', 'lon', 'time')
                monthly_dataset = utils.truncate_time_dim(monthly_dataset)
                monthly_dataset[input_dataset].attrs['units'] = dataset[input_dataset].units
                output_file_path = path.replace(input_dataset, 'monthly_' + input_dataset)
                utils.save_to_netcdf(monthly_dataset, output_file_path, logging_level=logging.WARN)
        merge_years('monthly_' + input_dataset, file_path)
    LOGGER.info('Calculating monthly average temperature.')
    files = [get_merged_dataset_path(file_path, 'monthly_' + x) for x in input_datasets]
    with xarray.open_mfdataset(files, chunks={'time': 10}, combine='by_coords') as dataset:
        dataset['avg_temp'] = (dataset.max_temp + dataset.min_temp) / 2
        dataset['avg_temp'].attrs['units'] = dataset.max_temp.units
        # Dimensions must be in this order to be accepted by the climate indices tool
        dataset = dataset.drop_vars(['max_temp', 'min_temp', 'crs'], errors='ignore').transpose('lat', 'lon', 'time')
        output_file_path = get_merged_dataset_path(file_path, 'monthly_avg_temp')
        utils.save_to_netcdf(dataset, output_file_path)


def calc_monthly_et_short_crop(file_path):
    LOGGER.info('Calculating monthly short crop evapotranspiration.')
    # Too much data to merge et_short_crop and then calculate monthly.
    # So we calculate monthly for each individual file then merge
    et_paths = glob.glob(file_path.format(dataset='et_short_crop', year='*', filetype='nc'))
    for et_path in et_paths:
        with xarray.open_dataset(et_path) as dataset:
            # If the last month is incomplete we should drop it
            dataset = drop_incomplete_months(dataset)
            monthly_et = dataset.resample(time='M').sum(skipna=False)
            monthly_et = utils.truncate_time_dim(monthly_et)
            monthly_et['et_short_crop'].attrs['units'] = dataset.et_short_crop.units
            output_file_path = et_path.replace('et_short_crop', 'monthly_et_short_crop')
            utils.save_to_netcdf(monthly_et, output_file_path)
    merge_years('monthly_et_short_crop', file_path)


def merge_years(dataset_name, file_path):
    LOGGER.info('Merging files for: ' + dataset_name)
    output_path = get_merged_dataset_path(file_path, dataset_name)
    inputs_path = file_path.format(dataset=dataset_name, year='*', filetype='nc')
    with xarray.open_mfdataset(inputs_path, chunks={'time': 10}, combine='by_coords', parallel=True, engine='h5netcdf') as dataset:
        # Dimensions must be in this order to be accepted by the climate indices tool
        dataset = dataset.drop_vars('crs', errors='ignore').transpose('lat', 'lon', 'time')
        if dataset_name not in DAILY_DATASETS:
            dataset = utils.truncate_time_dim(dataset)
        encoding = {'time': {'units': 'days since 1889-01', '_FillValue': None}}
        utils.save_to_netcdf(dataset, output_path, encoding=encoding)


def get_merged_dataset_path(file_path, dataset_name):
    merged_file_path = file_path.format(dataset=dataset_name, year='', filetype='a')
    return os.path.dirname(merged_file_path) + '/full_' + dataset_name + '.nc'


def combine_soil_moisture(file_path):
    # Soil moisture is given as two files - one is historical data and the other is recent data downloaded from BoM.
    # There is some overlap. We need to combine these files while giving precedence to the recent data downloaded from
    # BoM
    LOGGER.info('Merging files for: soil_moisture')
    archive_dataset_path = file_path.format(dataset='soil_moisture', year='archive', filetype='nc')
    realtime_dataset_path = file_path.format(dataset='soil_moisture', year='realtime', filetype='nc')
    output_file_path = get_merged_dataset_path(file_path, 'soil_moisture')
    try:
        archive_dataset = xarray.open_dataset(archive_dataset_path, chunks={'time': 10})
    except FileNotFoundError:
        logging.warning('Historical data for soil moisture not found. Proceeding with recent data only. If you meant '
                        'to include historical data, please place it at: ' + archive_dataset_path)
        shutil.copyfile(realtime_dataset_path, output_file_path)
        return
    archive_dataset = archive_dataset.drop_vars('time_bnds', errors='ignore')
    realtime_dataset = xarray.open_dataset(realtime_dataset_path, chunks={'time': 10})
    date = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    date = date - relativedelta(months=1)
    realtime_dataset = realtime_dataset.sel(time=slice('1800-01', date))
    combined = realtime_dataset.combine_first(archive_dataset)
    utils.save_to_netcdf(combined, output_file_path)


def combine_ndvi(file_path):
    # NDVI is given as a 1km resolution archive combined with a 300m resolution near real time dataset.
    # There are several entries per month, which need to be aggregated as well.
    LOGGER.info('Regridding NDVI')
    archive_ndvi_paths = glob.glob(file_path.format(dataset='ndvi', year='1km.archive.*', filetype='nc'))
    for ndvi_path in archive_ndvi_paths:
        regridded_dataset_path = ndvi_path.replace('1km', '5km')
        if os.path.isfile(regridded_dataset_path):
            continue
        regrid_ndvi(ndvi_path, regridded_dataset_path, {'time': 1, 'lat': 500, 'lon': 500})
    realtime_ndvi_paths = glob.glob(file_path.format(dataset='ndvi', year='300m.realtime.*', filetype='nc'))
    for ndvi_path in realtime_ndvi_paths:
        regridded_dataset_path = ndvi_path.replace('300m', '5km')
        if os.path.isfile(regridded_dataset_path):
            continue
        archive_regridded_dataset_path = regridded_dataset_path.replace('realtime', 'archive')
        if os.path.isfile(archive_regridded_dataset_path):
            continue
        regrid_ndvi(ndvi_path, regridded_dataset_path, {'lat': 200, 'lon': 200})

    # Combine archive and realtime parts
    LOGGER.info('Merging files for: ndvi')
    with xarray.open_mfdataset(
        file_path.format(dataset='ndvi', year='5km.archive.*', filetype='nc'),
        chunks={'time': 10},
        combine='by_coords'
    ) as archive_dataset:
        archive_dataset = archive_dataset.resample(time='1MS').mean()
        files = glob.glob(file_path.format(dataset='ndvi', year='5km.realtime.*', filetype='nc'))
        time_dim = xarray.Variable('time', [datetime.strptime(file.split('.')[2], '%Y-%m-%d') for file in files])
        if len(files) == 0:
            full_dataset = archive_dataset
        else:
            realtime_dataset = xarray.concat([xarray.open_dataset(f) for f in files], dim=time_dim)
            realtime_dataset = realtime_dataset.resample(time='1MS').mean()
            realtime_dataset['lat'] = archive_dataset.lat
            realtime_dataset['lon'] = archive_dataset.lon
            full_dataset = archive_dataset.combine_first(realtime_dataset)
        shape = read_shape('shapes/gadm36_AUS_0.shp')
        regions = [record.geometry for record in shape.records() if record.attributes['NAME_0'] == 'Australia']
        area = regionmask.Regions(regions)
        mask = area.mask(full_dataset.lon, full_dataset.lat, lon_name='lon', lat_name='lat')
        full_dataset = full_dataset.where(~numpy.isnan(mask))
        full_dataset = full_dataset.rename({'NDVI': 'ndvi', 'lat': 'latitude', 'lon': 'longitude'})
        utils.save_to_netcdf(full_dataset, 'D:/data/ndvi/full_ndvi.nc')


def read_shape(shapefile=None):
    if shapefile is None:
        shp_file = shapereader.natural_earth(resolution='110m', category='cultural',
                                             name='admin_1_states_provinces_lines')
    else:
        shp_file = shapefile
    return shapereader.Reader(shp_file)


def regrid_ndvi(input_path, output_path, chunks):
        with xarray.open_dataset(
            input_path, chunks=chunks, drop_variables=['crs', 'TIME_GRID'], mask_and_scale=False
        ) as dataset:
            dataset = dataset.where(dataset.lon >= 112.0, drop=True)
            dataset = dataset.where(dataset.lon <= 154.0, drop=True)
            dataset = dataset.where(dataset.lat >= -44.0, drop=True)
            dataset = dataset.where(dataset.lat <= -10, drop=True)
            dataset = dataset.where(dataset.NDVI != 254)
            dataset = dataset.chunk(chunks={'lat': -1})
            model_lat = numpy.arange(-44.0, -9.975, 0.05)
            dataset = dataset.interp(lat=model_lat)
            dataset = dataset.chunk(chunks={'lon': -1})
            model_lat = numpy.arange(112.0, 154.025, 0.05)
            dataset = dataset.interp(lon=model_lat)
            dataset['lat'].attrs['units'] = 'degrees_north'
            dataset['lat'].attrs['axis'] = 'Y'
            dataset['lon'].attrs['units'] = 'degrees_east'
            dataset['lon'].attrs['axis'] = 'X'
            utils.save_to_netcdf(dataset, output_path)


def drop_incomplete_months(dataset):
    last_date = dataset['time'].values[dataset['time'].values.size - 1].astype('<M8[D]').item()
    last_day_of_month = calendar.monthrange(last_date.year, last_date.month)[1]
    if last_date.day != last_day_of_month:
        last_valid_day = last_date.replace(
            month=last_date.month - 1,
            day=calendar.monthrange(last_date.year, last_date.month - 1)[1]
        )
        return dataset.sel(time=slice('1890-01-01', last_valid_day))
    return dataset


def avg_over_period(dataset_name, file_path, scale):
    LOGGER.info('Calculating {} over {} months.'.format(dataset_name, scale))
    new_var_name = '{}_{}'.format(dataset_name, scale)
    input_path = get_merged_dataset_path(file_path, dataset_name)
    output_path = get_merged_dataset_path(file_path, new_var_name)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=RuntimeWarning)
        with xarray.open_dataset(input_path) as dataset:
            # The rolling mean operation fails if the last chunk length is lower than scale / 2
            # But chunks still need to be used or the program will run out of memory
            chunk_length = math.ceil(scale/2)
            while True:
                last_chunk_length = dataset['time'].size % chunk_length
                if last_chunk_length == 0 or last_chunk_length >= scale/2 or last_chunk_length >= dataset['time'].size:
                    break
                chunk_length += 1
            dataset = dataset.chunk({'time': chunk_length})
            var = list(dataset.keys())[0]
            dataset[new_var_name] = dataset[var].rolling(time=scale, min_periods=scale).construct('window').mean('window')
            # This operation doesn't account for missing time entries. We need to remove results around those time gaps
            # that shouldn't have enough data to exist.
            time = dataset['time'].values
            dates_to_remove = []
            for i in range(len(time)):
                if i < scale-1:
                    dates_to_remove.append(time[i])
                    continue
                # Get slice of dates for the size of the window
                window_dates = time[i - scale + 1:i + 1]
                first_date = window_dates[0].astype('<M8[M]').item()
                last_date = window_dates[-1].astype('<M8[M]').item()
                if ((last_date.year - first_date.year) * 12 + last_date.month - first_date.month) >= scale:
                    dates_to_remove.append(time[i])
            dataset = dataset.drop_sel(time=dates_to_remove)
            # Drop all input variables and anything else that slipped in, we ONLY want the new CDI.
            keys = dataset.keys()
            for key in keys:
                if key != new_var_name:
                    dataset = dataset.drop_vars(key)
            dataset = dataset.dropna('time', how='all')
            utils.save_to_netcdf(dataset, output_path)


if __name__ == '__main__':
    main()
