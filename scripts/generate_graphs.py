from netCDF4 import Dataset, num2date
import numpy as np
import os
from mpl_toolkits.basemap import Basemap
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
import argparse
from datetime import datetime
from multiprocessing import Pool
from matplotlib.font_manager import FontProperties
import matplotlib
matplotlib.use('TkAgg')
from matplotlib import pyplot as plt, cm


NUM_PROCESSES = 4
TITLE_FONT = FontProperties(fname='fonts/Roboto-Medium.ttf', size=14)
SUBTITLE_FONT = FontProperties(fname='fonts/Roboto-LightItalic.ttf', size=12)
REGULAR_FONT = FontProperties(fname='fonts/Roboto-Light.ttf', size=13)


def main():
    # Gets options and then generates graphs. Records the start time, end time and elapsed time.
    start_time = datetime.now()
    print('Starting time: ' + str(start_time))
    options = get_options()
    generate_all_graphs(options)
    end_time = datetime.now()
    print('End time: ' + str(end_time))
    elapsed_time = end_time - start_time
    print('Elapsed time: ' + str(elapsed_time))


def get_options():
    """
    Gets command line arguments and returns them.
    Options are accessed via options.index_name, options.shape, etc.

    Required arguments: netcdf, var_name, output_file_base
    Optional arguments: overwrite, shape, start_date, end_date, title, subtitle, colormap, colorbar_label, min, max,
                        levels

    Run this with the -h (help) argument for more detailed information. (python generate_graphs.py -h)

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
        help='This tool will only produce graphs for dates between start_date and end_date. Dates should be given in '
             'the format of 2017-08. If only one is provided, all graphs before or after the date will be produced. If '
             'this option isn\'t used, default behavior is to generate all graphs.'
    )
    optional.add_argument(
        '--end_date',
        help='See --start_date.'
    )
    optional.add_argument(
        '--title',
        help='Sets the graph\'s title on the lower left.'
    )
    optional.add_argument(
        '--subtitle',
        help='Sets the graph\'s subtitle on the lower left.'
    )
    optional.add_argument(
        '--colormap',
        default='RdBu',
        help='The color map of the graph. See the following link for options: '
             'https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html'
    )
    optional.add_argument(
        '--colorbar_label',
        help='The label above the colorbar legend (usually an abbreviation of the index name).'
    )
    optional.add_argument(
        '--min',
        help='The minimum level for the plotted variable shown in the graph and colorbar.',
        type=int
    )
    optional.add_argument(
        '--max',
        help='The maximum level for the plotted variable shown in the graph and colorbar.',
        type=int
    )
    optional.add_argument(
        '--levels',
        help='The number of levels for the plotted variable shown in the graph and colorbar.',
        type=int
    )
    return parser.parse_args()


def generate_all_graphs(options):
    """
    Reads data from the netCDF file provided. For each time slice, call generate_graph to generate one image.
    Uses multiprocessing with one process per graph.
    Skips all time slices which are not between start_date and end_date, if these options are given.
    Skips existing images unless the overwrite option is used.

    :param options:
    :return:
    """
    # Create folder for results
    os.makedirs(os.path.dirname(options.output_file_base), exist_ok=True)

    # Open netCDF file
    with Dataset(options.netcdf, mode='r') as dataset:
        lat = dataset.variables['lat'][:]
        lon = dataset.variables['lon'][:]
        time = dataset.variables['time']
        drought_index = dataset.variables[options.var_name][:]

        start_date = datetime.strptime(options.start_date, '%Y-%m').date() if options.start_date else None
        end_date = datetime.strptime(options.end_date, '%Y-%m').date() if options.end_date else None

        graph_data = []

        # Go through each time slice
        for i in range(len(time)):
            # Skip this image if it's not between the start_date and end_date
            date = num2date(time[i], time.units, time.calendar)
            truncated_date = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0).date()
            if (start_date and truncated_date < start_date) or (end_date and truncated_date > end_date):
                continue

            # Skip existing images if the user has not chosen to overwrite them.
            file_path = '{}{}-{:02}.jpg'.format(options.output_file_base, date.year, date.month)
            if not options.overwrite and os.path.exists(file_path):
                continue

            # Add to list of images to be generated
            graph_data.append((drought_index[:, :, i], lat, lon, options, file_path, date))

    # Multiprocessing - one process per time slice
    pool = Pool(NUM_PROCESSES)
    pool.map(generate_graph, graph_data)
    pool.close()
    pool.join()


def generate_graph(graph_args):
    """
    All the arguments of this function are stored as a tuple for compatibility with map() and multiprocessing.
    Arguments:
    data - whichever index / variable is being graphed over the geographical area
    lat - array of latitude values
    lon - array of longitude values
    options - user-input options which control some things, including...
        shape - path to shape files which determines the map background. This path should have no extension. It's
                assumed all .shp, .sbf and .shx files will exist with this name. If this is null, a default Basemap
                background will be used
        title - main title of the graph (usually full name on the index)
        subtitle - subtitle (additional information if necessary)
        colormap - the color scheme to be used in the graph and colorbar
        colorbar_label - label to be printed above the colorbar legend (usually the index name)
    file_path - where the image will be saved
    date - the date of the time slice to which this graph belongs

    :param graph_args:
    :return:
    """
    # Unpack arguments
    data, lat, lon, options, file_path, date = graph_args
    # Set size of the plot and get figure and axes values for later reference
    fig, ax = plt.subplots(figsize=[8, 8])
    # Use custom shapefile if provided, otherwise use default Basemap. This prepares the graph for plotting.
    if options.shape:
        map_base = Basemap(resolution=None, llcrnrlon=lon.min(), llcrnrlat=lat.min(), urcrnrlon=lon.max(),
                           urcrnrlat=lat.max(), area_thresh=500, ax=ax)
        map_base.readshapefile(options.shape, 'Australia', linewidth=0.4)
    else:
        map_base = Basemap(resolution='f', llcrnrlon=lon.min(), llcrnrlat=lat.min(), urcrnrlon=lon.max(),
                           urcrnrlat=lat.max(), area_thresh=500, ax=ax)
        map_base.drawcoastlines(linewidth=0.4)

    # No border for this graph
    plt.axis('off')

    # Plot the data on the map
    lon, lat = np.meshgrid(lon, lat)
    colour_map = cm.get_cmap(options.colormap)
    if options.min is not None and options.max is not None and options.levels is not None:
        levels = np.linspace(options.min, options.max, options.levels)
        plot = map_base.contourf(lon, lat, data, levels, latlon=True, cmap=colour_map, extend="both")
    else:
        plot = map_base.contourf(lon, lat, data, latlon=True, cmap=colour_map, extend="both")

    # Add a colorbar on the top left. To control the size and position of the colorbar an inset axis is required.
    axins = inset_axes(ax, width='5%', height='50%', loc='lower left', bbox_to_anchor=(0.95, 0.7, 0.6, 0.5),
                       bbox_transform=ax.transAxes, borderpad=0)
    fig.colorbar(plot, cax=axins, extendrect=True, extendfrac='auto')

    # Add date of this graph, and title/subtitle/index name if given
    plt.text(.1, .05, date.strftime('%B %Y'), transform=ax.transAxes, fontproperties=REGULAR_FONT)
    if options.title:
        plt.text(.1, .1, options.title, transform=ax.transAxes, fontproperties=TITLE_FONT)
    if options.subtitle:
        plt.text(.1, .15, date.strftime(options.subtitle), transform=ax.transAxes, fontproperties=SUBTITLE_FONT)
    if options.colorbar_label:
        axins.set_title(options.colorbar_label, fontproperties=REGULAR_FONT)

    # Save graph
    plt.savefig(file_path, dpi=200, bbox_inches='tight')
    plt.close()


if __name__ == '__main__':
    main()