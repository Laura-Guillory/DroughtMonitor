import numpy as np
import os
import argparse
from datetime import datetime
import multiprocessing
from astropy.convolution import convolve, Gaussian2DKernel
import xarray
from colormaps import get_colormap
import utils
import logging
from mpl_toolkits.basemap import Basemap
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
import matplotlib
from matplotlib.font_manager import FontProperties
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon
matplotlib.use('TkAgg')
from matplotlib import pyplot as plt, cm

logging.basicConfig(level=logging.WARN, format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d  %H:%M:%S")
LOGGER = logging.getLogger(__name__)


TITLE_FONT = FontProperties(fname='fonts/Roboto-Medium.ttf', size=14)
SUBTITLE_FONT = FontProperties(fname='fonts/Roboto-LightItalic.ttf', size=12)
REGULAR_FONT = FontProperties(fname='fonts/Roboto-Light.ttf', size=13)
SMALL_FONT = FontProperties(fname='fonts/Roboto-Light.ttf', size=10)
OVERLAY_FONT = FontProperties(fname='fonts/Roboto-Medium.ttf', size=80)

COLORBAR_LABELS_X_OFFSET = 1.3


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
    Optional arguments: overwrite, shape, start_date, end_date, title, subtitle, colormap, colorbar_label, min, max,
                        levels

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
        '--colormap',
        default='RdBu',
        help='The color map of the map. See the following link for options: '
             'https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html'
    )
    optional.add_argument(
        '--colorbar_label',
        help='The label above the colorbar legend (usually an abbreviation of the index name).'
    )
    optional.add_argument(
        '--colorbar_ticklabels',
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
             'Example: 8, or 0 1 2 3 4 5 6 7',
        nargs="+",
        type=float
    )
    optional.add_argument(
        '--height',
        help='Height of desired map domain in projection coordinates (meters). If not provided a default will be '
             'estimated.',
        type=int
    )
    optional.add_argument(
        '--width',
        help='Width of desired map domain in projection coordinates (meters). If not provided a default will be '
             'estimated.',
        type=int
    )
    optional.add_argument(
        '-p', '--prototype',
        action='store_true',
        help='Adds an overlay to the image labelling it as a prototype.'
    )
    optional.add_argument(
        '--no_data',
        action='store_true',
        help='Adds a No Data portion to the colorbar legend. Use this if blank areas are common on this type of map.'
    )
    parser.add_argument(
        '-v', '--verbose',
        help='Increase output verbosity',
        action='store_const',
        const=logging.INFO,
        default=logging.WARN
    )
    parser.add_argument(
        '--multiprocessing',
        help='Number of processes to use in multiprocessing.',
        choices=["single", "all_but_one", "all"],
        required=False,
        default="all_but_one",
    )
    return parser.parse_args()


def generate_all_maps(options, number_of_worker_processes):
    """
    Reads data from the netCDF file provided. For each time slice, call generate_map to generate one image.
    Uses multiprocessing with one process per map.
    Skips all time slices which are not between start_date and end_date, if these options are given.
    Skips existing images unless the overwrite option is used.

    :param options:
    :return:
    """
    # Create folder for results
    os.makedirs(os.path.dirname(options.output_file_base), exist_ok=True)

    # Open netCDF file
    with xarray.open_dataset(options.netcdf, chunks={'time': 10}) as dataset:
        start_date = datetime.strptime(options.start_date, '%Y-%m').date() if options.start_date else None
        end_date = datetime.strptime(options.end_date, '%Y-%m').date() if options.end_date else None
        map_data = []

        # Make sure coordinate dims exist
        dataset = dataset.dropna('time', how='all')
        lon, lat = utils.get_lon_lat_names(dataset)

        for date, data_slice in dataset[options.var_name].groupby('time'):
            date = date.astype('<M8[M]').item()

            # Skip this image if it's not between the start_date and end_date
            if (start_date and date < start_date) or (end_date and date > end_date):
                continue

            # Skip existing images if the user has not chosen to overwrite them.
            file_path = '{}{}-{:02}.jpg'.format(options.output_file_base, date.year, date.month)
            if not options.overwrite and os.path.exists(file_path):
                continue

            # Add to list of images to be generated
            map_data.append((data_slice, dataset[lat], dataset[lon], options, file_path, date))

    # Multiprocessing - one process per time slice
    pool = multiprocessing.Pool(number_of_worker_processes)
    pool.map(generate_map, map_data)
    pool.close()
    pool.join()


def generate_map(map_args):
    """
    All the arguments of this function are stored as a tuple for compatibility with map() and multiprocessing.
    Arguments:
    data - whichever index / variable is being mapped over the geographical area
    lat - array of latitude values
    lon - array of longitude values
    options - user-input options which control some things, including...
        shape - path to shape files which determines the map background. This path should have no extension. It's
                assumed all .shp, .sbf and .shx files will exist with this name. If this is null, a default Basemap
                background will be used
        title - main title of the map (usually full name on the index)
        subtitle - subtitle (additional information if necessary)
        colormap - the color scheme to be used in the map and colorbar
        colorbar_label - label to be printed above the colorbar legend (usually the index name)
    file_path - where the image will be saved
    date - the date of the time slice to which this map belongs

    :param map_args:
    :return:
    """
    # Unpack arguments
    data, lat, lon, options, file_path, date = map_args
    # Set size of the plot and get figure and axes values for later reference
    fig, ax = plt.subplots(figsize=[7, 7])
    # Use custom shapefile if provided, otherwise use default Basemap. This prepares the map for plotting.
    if options.width and options.height:
        map_base = Basemap(resolution='l', projection='lcc', lon_0=lon.mean(), lat_0=lat.mean(),
                           width=options.width, height=options.height, area_thresh=500, ax=ax)
    else:
        map_base = Basemap(resolution='l', projection='lcc', lon_0=lon.mean(), lat_0=lat.mean(),
                           llcrnrlat=lat.min(), llcrnrlon=lon.min(), urcrnrlat=lat.max(), urcrnrlon=lon.max(),
                           area_thresh=500, ax=ax)

    # Continent outline
    if options.shape:
        map_base.readshapefile(options.shape, 'Australia', linewidth=0.4)
        # Grey background
        patches = [Polygon(np.array(shape), True) for info, shape in zip(map_base.Australia_info, map_base.Australia)]
        ax.add_collection(PatchCollection(patches, facecolor='#afafaf'))
    else:
        map_base.drawcoastlines(linewidth=0.5)
        map_base.drawstates(linewidth=0.5)
        map_base.fillcontinents(color='#afafaf', zorder=0)

    # No border for this map
    plt.axis('off')

    # Get the colour map
    colour_map = get_colormap(options.colormap)
    if colour_map is None:
        colour_map = cm.get_cmap(options.colormap)

    # Smooth contours on map
    data = convolve(data, Gaussian2DKernel(x_stddev=2), boundary='extend', preserve_nan=True)

    # Plot the data on the map
    lon, lat = np.meshgrid(lon, lat)
    if options.levels is not None and len(options.levels) > 1:
        plot = map_base.contourf(lon, lat, data, options.levels, latlon=True, cmap=colour_map, extend="both")
    elif options.min is not None and options.max is not None and options.levels is not None:
        levels = np.linspace(options.min, options.max, options.levels[0])
        plot = map_base.contourf(lon, lat, data, levels, latlon=True, cmap=colour_map, extend="both")
    else:
        plot = map_base.contourf(lon, lat, data, latlon=True, cmap=colour_map, extend="both")

    # Add a colorbar on the top left. To control the size and position of the colorbar an inset axis is required.
    axins = inset_axes(ax, width='5%', height='50%', loc='lower left', bbox_to_anchor=(0.95, 0.7, 0.6, 0.5),
                       bbox_transform=ax.transAxes, borderpad=0)
    colorbar = plt.colorbar(plot, cax=axins, extendfrac='auto', extendrect=True)
    if options.colorbar_ticklabels is not None:
        colorbar.ax.get_yaxis().set_ticks([])
        tick_labels = [x.strip() for x in options.colorbar_ticklabels.split(',')]
        for i, label in enumerate(tick_labels):
            # Automatically positioning n labels is tricky
            num_labels = len(tick_labels)
            y_spread = -0.0003 * num_labels**3 + 0.0139 * num_labels**2 - 0.1986 * num_labels + 1.0133
            y_offset = -0.0033 * num_labels**2 + 0.0733 * num_labels - 0.4751
            colorbar.ax.text(COLORBAR_LABELS_X_OFFSET, i * y_spread + y_offset, label, ha='left', va='center',
                             fontproperties=SMALL_FONT)

    # Add extra colorbar segment for no data if required
    if options.no_data:
        nodata_axins = inset_axes(ax, width='5%', height = '50%', loc='lower left',
                                  bbox_to_anchor=(0.95, 0.65, 0.6, 0.05), bbox_transform=ax.transAxes, borderpad=0)
        nodata_cmap = matplotlib.colors.ListedColormap(['#afafaf'])
        nodata_colorbar = matplotlib.colorbar.ColorbarBase(nodata_axins, cmap=nodata_cmap, extend='neither',
                                                           ticks=[1, 2])
        nodata_colorbar.ax.get_yaxis().set_ticks([])
        nodata_colorbar.ax.text(COLORBAR_LABELS_X_OFFSET, 0.5, 'No data', ha='left', va='center',
                                fontproperties=SMALL_FONT)

    # Add date of this map, and title/subtitle/index name if given
    plt.text(.1, .05, date.strftime('%B %Y'), transform=ax.transAxes, fontproperties=REGULAR_FONT)
    if options.title:
        plt.text(.1, .1, options.title, transform=ax.transAxes, fontproperties=TITLE_FONT)
    if options.subtitle:
        plt.text(.1, .15, date.strftime(options.subtitle), transform=ax.transAxes, fontproperties=SUBTITLE_FONT)
    if options.colorbar_label:
        axins.set_title(options.colorbar_label, fontproperties=REGULAR_FONT)

    # Add prototype overlay if requested
    if options.prototype:
        plt.text(0.5, 0.5, "PROTOTYPE", transform=ax.transAxes, alpha=.15, fontproperties=OVERLAY_FONT,
                 horizontalalignment='center', verticalalignment='center')

    # Save map
    plt.savefig(file_path, dpi=150, bbox_inches='tight', quality=80)
    plt.close()


if __name__ == '__main__':
    main()
