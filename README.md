# Climate Data Tools

This repository contains tools for downloading gridded Australian climate data from 
[LongPaddock's SILO](https://silo.longpaddock.qld.gov.au/) in a netCDF format and processing that data.

## Requirements

* Python 3
* Anaconda
* Install the packages listed in requirements.txt

Scripts can be found in the `scripts` directory. Please be advised they require a ton of disk space and time.

### download.py

Run download.py to download all the data, which will be stored in the newly created `data` directory. Any previously 
downloaded files will be skipped, so you can run this again to update your copy of the data without redownloading 
anything but the current year. 

```
Usage: python download.py --path PATH --datasets dataset1 dataset2
```

Arguments:  

|||
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --path             | Determines where the data will be saved. Defaults to data/{dataset}/{year}.{dataset}.nc                                                                                                        |
| --datasets         | Which datasets to download. This argument is required. Accepts multiple arguments. Check DATASET_CHOICES inside script to see options for this argument.                                                                                                                    |


If you cancel this script midway through it's likely that the most recent file will be empty/corrupt so you should 
delete it before running again.

### prep_files.py

Prepares files for use in calculations such as [climate indices](https://github.com/monocongo/climate_indices). This 
consolidates all the files for each year into a single one, and reorders the dimensions into (lat, lon, time). 

This script can also calculate an average temperature dataset in both daily and monthly format, as long as minimum and
maximum temperature datasets are present.

```
Usage: python download.py --path PATH --datasets dataset1 dataset2
```

Arguments:  

|||
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --path             | Determines where the input files can be found. Defaults to data/{dataset}/{year}.{dataset}.nc. Output will be saved in the same directory as 'full_{dataset}.nc'                                                                |
| --datasets         | Which datasets to prepare. This argument is required. Accepts multiple arguments. Check DOWNLOADED_DATASETS and COMPUTED_DATASETS inside script to see options for this argument.

### generate_graphs.py

This tool accepts a netCDF file of georeferenced data as input and generates visual graphs from that data. It generates
one graph per time slice. Graphs will have a colour bar legend with a customisable colour bar label, main title, and
subtitle. Graphs will be saved in a directory defined by the `output_file_base` option.

```
Usage: python generate_graphs.py --netcdf NETCDF_PATH --var_name VAR_NAME --output_file_base OUTPUT_FILE_BASE
```

Required arguments:  

|||
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --netcdf           | The path of the netCDF file containing the data.                                                                                                            |
| --index_name       | The name of the variable to plot.                                                                                                                           |
| --output_file_base | Base file name for all output files. Each image file will begin with this base name plus the date of the time slice. (e.g. SPI-1 becomes SPI-1_1889-01.jpg) |

Optional arguments:

|||
|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| -o, --overwrite  | Existing images will be overwritten. Default behavior is to skip existing images.                                                                                                                                                                                              |
| --shape          | The path of an optional shape file to use for the base map, such as from [gadm.org](https://gadm.org/). If not provided, a default will be provided.                                                                                                                           |
| --start_date     | This tool will only produce graphs for dates between start_date and end_date. Dates should be given in the format of 2017-08. Both start_date and end_date arguments must be present for this to work. If this option  isn't used, default behavior is to generate all graphs. |
| --end_date       | See --start_date                                                                                                                                                                                                                                                               |
| --title          | Sets the graph's title on the lower left. |
| --subtitle       | Sets the graph's subtitle on the lower left |
| --colormap       | The color map of the graph. See the following link for options: https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html' |
| --colorbar_label | The label above the coloarbar legend (usually an abbreviation of the index name) |
| --min            | The minimum level for the plotted variable shown in the graph and colorbar. |
| --max            | The maximum level for the plotted variable shown in the graph and colorbar. |
| --levels         | The number of levels for the plotted variable shown in the graph and colorbar. |

## Contacts

**Laura Guillory**  
_Web Developer_  
Centre for Applied Climate Science  
University of Southern Queensland  
[laura.guillory@usq.edu.au](mailto:laura.guillory@usq.edu.au)
