# Climate Data Tools

This repository contains Tools for obtaining and processing gridded climate data from [LongPaddock's SILO](https://silo.longpaddock.qld.gov.au/), and other netCDF files.

## Requirements

* Python 3
* Anaconda
* Install the packages listed in requirements.txt

Scripts can be found in the `scripts` directory. Please be advised it can take some time to process large volumes of 
data.

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
| --colormap       | The color map of the graph. See [Matlib's Colormaps](https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html) for options. |
| --colorbar_label | The label above the coloarbar legend (usually an abbreviation of the index name) |
| --min            | The minimum level for the plotted variable shown in the graph and colorbar. |
| --max            | The maximum level for the plotted variable shown in the graph and colorbar. |
| --levels         | The number of levels for the plotted variable shown in the graph and colorbar. |

### truncate_time_dim.py

This tool will go through each time entry of a netCDF file and truncate the datetime value to be at the beginning of the
month. 

e.g.  
2018-09-06 -> 2018-09-01  
2018-10-07 -> 2018-10-01  
2018-11-30 -> 2018-11-01  

This can be useful if you are processing multiple datasets with one entry per month and experiencing issues due to a slight date mismatch.

|||
|----------|---------------------------------------------------------------------------------------|
| --input  | The path of the file to use as input (required)                                                 |
| --output | The location to save the result. If not supplied, the input file will be overwritten. |

### transpose.py

Saves the dimensions of a netCDF file in a different order, because some programs will expect the dimensions to be
ordered a specific way and won't run without it.

Incomplete.

### esrigrid2netcdf.py

Converts a folder of data in ESRI gridded format and convert it into a single netCDF file.

Incomplete.

## Contacts

**Laura Guillory**  
_Web Developer_  
Centre for Applied Climate Science  
University of Southern Queensland  
[laura.guillory@usq.edu.au](mailto:laura.guillory@usq.edu.au)
