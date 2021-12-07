import cartopy
from shapely.ops import cascaded_union
from cartopy.io import shapereader
import os
import argparse
import numpy
from datetime import datetime
from dateutil.relativedelta import relativedelta
import multiprocessing
import xarray
import utils
import logging
import matplotlib
from matplotlib.font_manager import FontProperties
from matplotlib import pyplot
import matplotlib.patches
import warnings
import rioxarray
from descartes import PolygonPatch

logging.basicConfig(level=logging.WARN, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d  %H:%M:%S")
LOGGER = logging.getLogger(__name__)

TITLE_FONT = FontProperties(fname='fonts/Roboto-Medium.ttf', size=14)
SUBTITLE_FONT = FontProperties(fname='fonts/Roboto-LightItalic.ttf', size=12)
REGULAR_FONT = FontProperties(fname='fonts/Roboto-Light.ttf', size=13)
SMALL_FONT = FontProperties(fname='fonts/Roboto-Light.ttf', size=10)
OVERLAY_FONT = FontProperties(fname='fonts/Roboto-Medium.ttf', size=70)

COLORBAR_LABELS_X_OFFSET = 1.3

REGIONS = {
    'SEQ': {'Brisbane', 'Moreton Bay', 'Logan', 'Ipswich', 'Redland', 'Scenic Rim', 'Somerset', 'Lockyer Valley',
            'Gold Coast', 'Sunshine Coast', 'Toowoomba'}
}


def main():
    # Gets options and then generates maps. Records the start time, end time and elapsed time.
    options = get_options()
    LOGGER.setLevel(options.verbose)
    start_time = datetime.now()
    LOGGER.info('Starting time: ' + str(start_time))

    if options.multiprocessing == "single":
        number_of_worker_processes = 1
    elif options.multiprocessing == "all":
        number_of_worker_processes = multiprocessing.cpu_count()
    else:
        number_of_worker_processes = multiprocessing.cpu_count() - 1

    generate_all_maps(options, number_of_worker_processes)
    end_time = datetime.now()
    LOGGER.info('End time: ' + str(end_time))
    elapsed_time = end_time - start_time
    LOGGER.info('Elapsed time: ' + str(elapsed_time))


def get_options():
    """
    Gets command line arguments and returns them.
    Options are accessed via options.index_name, options.shape, etc.

    Required arguments: netcdf, var_name, output_file_base
    Optional arguments: overwrite, shape, start_date, end_date, title, subtitle, label_position, colours,
                        colourbar_label, colourbar_position, categories, min, max, levels, region, extent, prototype,
                        no_data, verbose, multiprocessing, time_window, time_window_type

    Run this with the -h (help) argument for more detailed information. (python generate_maps.py -h)

    :return:
    """
    parser = argparse.ArgumentParser()
    parser._action_groups.pop()
    required = parser.add_argument_group('Required arguments')
    optional = parser.add_argument_group('Optional arguments')
    required.add_argument(
        '--netcdf',
        required=True,
        help='The path of the netCDF file containing the data.'
    )
    required.add_argument(
        '--var_name',
        required=True,
        help='The name of the variable to plot.')
    required.add_argument(
        '--output_file_base',
        help='Base file name for all output files. Each image file will begin with this base name plus the date of the '
             'time slice. (e.g. SPI-1 becomes SPI-1_1889-01.jpg)'
    )
    optional.add_argument(
        '-o', '--overwrite',
        action='store_true',
        help='Existing images will be overwritten. Default behavior is to skip existing images.'
    )
    optional.add_argument(
        '--shape',
        help='The path of an optional shape file to use for the base map, such as from gadm.org. If not provided, a '
             'default will be provided.')
    optional.add_argument(
        '--start_date',
        help='This tool will only produce maps for dates between start_date and end_date. Dates should be given in '
             'the format of 2017-08. If only one is provided, all maps before or after the date will be produced. If '
             'this option isn\'t used, default behavior is to generate all maps.'
    )
    optional.add_argument(
        '--end_date',
        help='See --start_date.'
    )
    optional.add_argument(
        '--title',
        help='Sets the map\'s title on the lower left.'
    )
    optional.add_argument(
        '--subtitle',
        help='Sets the map\'s subtitle on the lower left.'
    )
    optional.add_argument(
        '--label_position',
        help='Sets the position of the title, subtitle and date on the image. Given as a fraction of the image from '
             'the bottom-left corner. Default: 0.1 0.05',
        nargs='+',
        type=float,
        default=[.1, .05]
    )
    optional.add_argument(
        '--colours',
        default=None,
        nargs="+",
        help='A list of hex colours to use for the map, from lowest value to highest. There should be one more colour '
             'than there are levels.'
    )
    optional.add_argument(
        '--colourbar_label',
        help='The label above the colourbar legend (usually an abbreviation of the index name).'
    )
    optional.add_argument(
        '--colourbar_position',
        help='Sets the position of the colourbar. Given as a fraction of the image from the bottom-left corner. '
             'Default: 0.807 0.6',
        nargs='+',
        type=float,
        default=[0.807, 0.6]
    )
    optional.add_argument(
        '--categories',
        help='Labels to replace the numbered levels on the colorbar.'
    )
    optional.add_argument(
        '--min',
        help='The minimum level for the plotted variable shown in the map and colorbar.',
        type=float
    )
    optional.add_argument(
        '--max',
        help='The maximum level for the plotted variable shown in the map and colorbar.',
        type=float
    )
    optional.add_argument(
        '--levels',
        help='If one number is given, it is the number of levels for the plotted variable shown in the map and '
             'colorbar. If multiple numbers are given, they will be used as a list to explicitly set each level.'
             'Example: 8, or 0 1 2 3 4 5 6 7 8',
        nargs="+",
        type=float
    )
    optional.add_argument(
        '--region',
        help='Mask out all of the states except the one specified by this argument (e.g. "Queensland" or '
             '"Northern Territory")',
        default=None
    )
    optional.add_argument(
        '--extent',
        help='Defines the extent of the map in latitude and longitude. Should be given as four values: left, right, '
             'bottom and top. Example: 137, 155, -10, -30',
        nargs='+',
        type=float
    )
    optional.add_argument(
        '-p', '--prototype',
        action='store_true',
        help='Adds an overlay to the image labelling it as a prototype.'
    )
    optional.add_argument(
        '--no_data',
        action='store_true',
        help='Adds a No Data section to the colorbar legend. Use this if blank areas are common on this type of map.'
    )
    optional.add_argument(
        '-v', '--verbose',
        help='Increase output verbosity',
        action='store_const',
        const=logging.INFO,
        default=logging.WARN
    )
    optional.add_argument(
        '--multiprocessing',
        help='Number of processes to use in multiprocessing.',
        choices=["single", "all_but_one", "all"],
        default="all_but_one",
    )
    optional.add_argument(
        '--no_downsampling',
        help='Don\'t downsample data to generate maps faster. This option can be helpful if downsampling is causing '
             'blurred borders.',
        action='store_const',
        const=True,
        default=False
    )
    optional.add_argument(
        '--plot',
        help='What method to use to plot the data. Options: pcolor, contour.',
        default='contour',
        type=str,
        choices=['contour', 'pcolor']
    )
    optional.add_argument(
        '--time_window',
        help='The number of months that this map is portraying. (e.g. 3)',
        default=1,
        type=int
    )
    optional.add_argument(
        '--time_window_type',
        help='Used to determine whether the date of a map is the beginning or the end of the time window, when the '
             '--time_window option is used. Options: beginning, end.',
        choices=['beginning', 'end'],
        default='beginning',
        type=str
    )
    return parser.parse_args()


def generate_all_maps(options, number_of_worker_processes):
    """
    Reads data from the netCDF file provided. For each time slice, call generate_map to generate one image.
    Uses multiprocessing with one process per map.
    Skips all time slices which are not between start_date and end_date, if these options are given.
    Skips existing images unless the overwrite option is used.

    :param options:
    :param number_of_worker_processes:
    :return:
    """
    # Verify arguments
    if options.time_window < 1:
        raise ValueError('--time_window must 1 or larger.')

    # Create folder for results
    os.makedirs(os.path.dirname(options.output_file_base), exist_ok=True)

    # Open netCDF file
    with xarray.open_dataset(options.netcdf) as dataset:
        if 'time' in dataset.dims:
            dataset = dataset.chunk({'time': 10})

        # Get labels for latitude and longitude
        lon_label, lat_label = utils.get_lon_lat_names(dataset)

        if options.region is None:
            if not options.no_downsampling:
                dataset = dataset.coarsen(dim={'time': 1, lat_label: 3, lon_label: 3}, boundary='pad').mean()
        else:
            shape = read_shape(options.shape)
            if options.region in REGIONS:
                regions = [record.geometry for record in shape.records() if record.attributes['NAME_2'] in REGIONS[options.region]]
            else:
                regions = [record.geometry for record in shape.records() if record.attributes['NAME_1'] == options.region]
            if len(regions) == 0:
                raise ValueError('That region does not exist in that shapefile.')
            dataset = dataset.ffill(lon_label, limit=1).bfill(lon_label, limit=1)
            dataset = dataset.ffill(lat_label, limit=1).bfill(lat_label, limit=1)
            dataset.rio.write_crs('epsg:4326', inplace=True)
            dataset = dataset.rio.clip(regions, all_touched=True)

        latitude = {
            'min': dataset[lat_label].min().item(),
            'mean': dataset[lat_label].mean().item(),
            'max': dataset[lat_label].max().item(),
            'label': lat_label
        }
        longitude = {
            'min': dataset[lon_label].min().item(),
            'mean': dataset[lon_label].mean().item(),
            'max': dataset[lon_label].max().item(),
            'label': lon_label
        }

        if 'time' not in dataset.dims:
            generate_map((dataset, latitude, longitude, options, options.output_file_base + '.jpg', None))
            return

        start_date = datetime.strptime(options.start_date, '%Y-%m').date() if options.start_date else None
        end_date = datetime.strptime(options.end_date, '%Y-%m').date() if options.end_date else None
        map_data = []

        for date, data_slice in dataset.groupby('time'):
            date = date.astype('<M8[M]').item()

            # Skip this image if it's not between the start_date and end_date
            if (start_date and date < start_date) or (end_date and date > end_date):
                continue

            # Skip existing images if the user has not chosen to overwrite them.
            file_path = '{}{}-{:02}.jpg'.format(options.output_file_base, date.year, date.month)
            if not options.overwrite and os.path.exists(file_path):
                continue

            # Add to list of images to be generated
            map_data.append((data_slice, latitude, longitude, options, file_path, date))

    # Multiprocessing - one process per time slice
    pool = multiprocessing.Pool(number_of_worker_processes)
    pool.map(generate_map, map_data)
    pool.close()
    pool.join()


def generate_map(map_args):
    # Unpack arguments
    data, latitude, longitude, options, file_path, date = map_args
    if options.region is None:
        projection = cartopy.crs.LambertConformal(
            central_longitude=longitude['mean'],
            central_latitude=latitude['mean'],
            standard_parallels=(-10, -40),
            cutoff=latitude['max']+2
        )
    else:
        projection = cartopy.crs.PlateCarree()

    if options.extent is not None:
        left, right, bottom, top = options.extent
    else:
        left = longitude['min']
        right = longitude['max']
        bottom = latitude['min']
        top = latitude['max']

    figure = pyplot.figure(figsize=(8, 8))  # Set size of the plot
    ax = pyplot.axes(projection=projection, extent=(left, right, bottom, top+2))
    pyplot.gca().outline_patch.set_visible(False)  # Remove border around plot

    # Get the shape reader
    shape = read_shape(options.shape)

    # Draw grey background
    if options.region is None:
        area = shape.geometries()
    else:
        if options.region in REGIONS:
            area = [record.geometry for record in shape.records() if record.attributes['NAME_2'] in REGIONS[options.region]]
        else:
            area = [record.geometry for record in shape.records() if record.attributes['NAME_1'] == options.region]
    ax.add_geometries(area, cartopy.crs.PlateCarree(), edgecolor='none', facecolor='#afafaf', linewidth=1, zorder=0)

    # Plot the data
    if options.levels is not None and len(options.levels) > 1:
        levels = options.levels
    elif options.min is not None and options.max is not None and options.levels is not None:
        levels = numpy.linspace(options.min, options.max, options.levels[0])
    else:
        levels = None
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=RuntimeWarning)
        if options.plot == 'contour':
            im = ax.contourf(data[longitude['label']], data[latitude['label']], data[options.var_name], extend='both',
                             transform=cartopy.crs.PlateCarree(), colors=options.colours, levels=levels, zorder=1)
            # On smaller maps, bleeding over borders is visible due to a coarse resolution. This code masks the plot at
            # the border but it's not necessary on larger maps.
            if options.region is not None:
                mask = PolygonPatch(cascaded_union(area), transform=ax.transData)
                for c in im.collections:
                    c.set_clip_path(mask)
        else:
            cmap = matplotlib.colors.ListedColormap(options.colours)
            im = ax.pcolormesh(data[longitude['label']], data[latitude['label']], data[options.var_name], cmap=cmap,
                               transform=cartopy.crs.PlateCarree(), zorder=1)

    # Draw borders
    for state in shape.records():
        if options.region is None or state.attributes['NAME_1'] == options.region:
            ax.add_geometries([state.geometry], cartopy.crs.PlateCarree(), edgecolor='black', facecolor='none',
                              linewidth=0.4, zorder=3)
    if options.region in REGIONS:
        ax.add_geometries(area, cartopy.crs.PlateCarree(), edgecolor='black', facecolor='none',
                          linewidth=0.4, zorder=3)

    # Add a colourbar
    colourbar_axis = figure.add_axes([options.colourbar_position[0], options.colourbar_position[1] + 0.03,
                                      .019, .16])
    colourbar = figure.colorbar(im, cax=colourbar_axis, extendfrac=0)
    if options.categories is not None:
        colourbar.ax.tick_params(axis='both', which='both', length=0)
        if options.plot == 'contour':
            colourbar.set_ticks([(options.levels[i] + options.levels[i+1])/2 for i in range(0, len(options.levels)-1)])
        else:
            colourbar.set_ticks([x*0.85 + 0.5 for x in levels])
        colourbar.set_ticklabels(options.categories.split(', '))
    for tick in colourbar_axis.get_yticklabels():
        tick.set_font_properties(SMALL_FONT)

    # Add extra colorbar segment for no data if required
    if options.no_data:
        nodata_axis = figure.add_axes([options.colourbar_position[0], options.colourbar_position[1], .019,
                                       .016])
        nodata_cmap = matplotlib.colors.ListedColormap(['#afafaf'])
        matplotlib.colorbar.ColorbarBase(nodata_axis, cmap=nodata_cmap, extend='neither')
        nodata_axis.get_yaxis().set_ticks([])
        nodata_axis.text(1.3, .4, 'No data', ha='left', va='center', fontproperties=SMALL_FONT)

    # Add date of this map, and title/subtitle/index name if given
    if date is not None:
        if options.time_window is not 1:
            if options.time_window_type == 'beginning':
                date2 = date + relativedelta(months=+(options.time_window - 1))
                date_str = date.strftime('%B %Y') + ' - ' + date2.strftime('%B %Y')
            else:
                date2 = date + relativedelta(months=-(options.time_window - 1))
                date_str = date2.strftime('%B %Y') + ' - ' + date.strftime('%B %Y')
        else:
            date_str = date.strftime('%B %Y')
        pyplot.text(options.label_position[0], options.label_position[1], date_str, transform=ax.transAxes,
                    fontproperties=REGULAR_FONT)
    label_space = .04 if options.region in REGIONS else .05
    if options.title:
        pyplot.text(options.label_position[0], options.label_position[1] + label_space, options.title,
                    transform=ax.transAxes, fontproperties=TITLE_FONT)
    if options.subtitle:
        pyplot.text(options.label_position[0], options.label_position[1] + label_space*2, options.subtitle,
                    transform=ax.transAxes, fontproperties=SUBTITLE_FONT)
    if options.colourbar_label:
        colourbar_axis.set_title(options.colourbar_label, fontproperties=REGULAR_FONT)

    # Add prototype overlay if requested
    if options.prototype:
        pyplot.text(.55, .4, "PROTOTYPE", transform=ax.transAxes, alpha=.15, fontproperties=OVERLAY_FONT,
                    horizontalalignment='center', verticalalignment='center')

    # Save map
    pyplot.savefig(file_path, dpi=150, bbox_inches='tight', quality=80)
    pyplot.close()


def read_shape(shapefile=None):
    if shapefile is None:
        shp_file = shapereader.natural_earth(resolution='110m', category='cultural',
                                             name='admin_1_states_provinces')
    else:
        shp_file = shapefile
    return shapereader.Reader(shp_file)


if __name__ == '__main__':
    main()
