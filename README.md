# Drought Monitor

Drought is one of the most severe natural disasters Australia faces, inflicting serious impacts on the agricultural 
industry. An Australia-wide Drought Monitor was developed to provide detailed and timely data regarding drought 
conditions to aid producers and policy makers alike. The Drought Monitor development is an integral part of the 
Northern Australia Climate Program (NACP), a major partnership between Meat & Livestock Australia, the Queensland 
Government and the University of Southern Queensland. 

The Australian Drought Monitor is based on the U.S. Drought Monitor (USDM) concept, which was developed by Mark 
Svoboda and his team at the National Drought Mitigation Center at the University of Nebraska-Lincoln in the late 
1990s. The Composite Drought Indicator (CDI) used for the NACP project is a scaled down version of the U.S. Drought 
Monitor, using only four selected indices. While the U.S. Drought Monitor depends on observations from more than 350 
contributors around the U.S, the Australian Drought Monitor is designed to make the best use of satellite-based 
spatial data sources to deliver an impartial drought monitoring service customised for Australia's unique climate.

The CDI is based on the combination of four different indices/indicators: 3-month Standard Precipitation Index (SPI), 
Soil Moisture (SM), Evapotranspiration (ET) and Normalised Difference Vegetation Index (NDVI), and is available at 
several timescales including 1, 3, 6, 9, 12, 24 and 36 months.

Results are processed and published monthly to the [Drought Monitor](https://nacp.org.au/drought_monitor) page on 
NACP's website.

This project contains the software necessary for producing the CDI. 

Features of this software package include:
* Downloading data from source
* Preprocessing data
* Percentile ranking datasets
* Calculating CDI
* Generating maps of Australia

This software is implemented in Python and designed for handling netCDF files. Please be advised that large volumes of 
data may take a long time to process without the use of an HPC.

Input datasets for the CDI are: NDVI, SPI-3, Soil Moisture, and ET short crop. Data is sourced from 
[LongPaddock's SILO](https://silo.longpaddock.qld.gov.au/), the Bureau of Meteorology 
[(Soil Moisture)](http://www.bom.gov.au/water/landscape), and the Copernicus Global Land Service 
([NDVI](https://land.copernicus.eu/global/products/ndvi))

This software package does not calculate SPI-3, which is a prerequisite for the CDI. It is suggested to use 
[this standard Climate Indices package](https://github.com/monocongo/climate_indices) to calculate SPI-3 from the 
rainfall data that this package will download for you from LongPaddock's SILO.

## Requirements

* Python 3
* Anaconda
* Install the packages listed in requirements.txt
* 7zip

Scripts can be found in the `scripts` directory.

## How to calculate CDI

1. Download the data

    Run download.py to download the input datasets. Suggested command: 
    ```
       python download.py -v --datasets monthly_rain et_short_crop ndvi soil_moisture
    ```
    
    This will download the input data from 
    [LongPaddock's SILO (Rainfall, Evapotranspiration)](https://silo.longpaddock.qld.gov.au/), 
    [Copernicus Land Service (NDVI)](https://land.copernicus.eu/global/products/ndvi), and the [Bureau of Meteorology 
    (Soil Moisture)](http://www.bom.gov.au/water/landscape) and save the files in the default directory, `data`.
   
    Note: This will download the realtime dataset for soil moisture, which only provides data back to 2000. For best 
    results, visit the [source](http://www.bom.gov.au/water/landscape) and enquire via email for the complete monthly 
    model output for root zone soil moisture. 

2. Prepare the data

    The data is downloaded as-is and is not ready to be used to calculate the CDI. Some of the datasets are downloaded 
    in a different spatial resolution (300m or 1km), some are split into dozens of files, some are daily data instead 
    of monthly, etc. This will convert the data into netCDF format with one file per dataset, with a spacial resolution 
    of 5km.
    
    ```
    python prep_files.py -v --datasets monthly_rain monthly_et_short_crop ndvi soil_moisture
    ```
   
3. Calculate SPI-3
    
    We now have the monthly_rain dataset prepared but need to calculate the SPI-3 to use for the CDI. It is suggested 
    to use [this standard Climate Indices package](https://github.com/monocongo/climate_indices).
    
    3.1 - Download Climate Indices  
    3.2 - Install Climate Indices according to the documentation 
    [here](https://climate-indices.readthedocs.io/en/latest/).  
    3.3 - Calculate SPI-3 using the following command:
    ```
    process_climate_indices --index spi --periodicity monthly --netcdf_precip {monthly_rain file} --var_name_precip monthly_rain --output_file_base spi_3.nc --scales 3 --calibration_start_year 1910 --calibration_end_year 2018
    ```
    Where `{monthly_rain file}` is the path to the netCDF file containing the monthly_rain dataset that we created in 
   step 2.
   
4. Calculate CDI

    We can now combine the four datasets we've prepared to calculate the CDI.
    ```
    python calculate_cdi.py -v --ndvi data/ndvi/full_ndvi.nc --ndvi_var ndvi --spi {spi_3 file} --spi_var spi_pearson_03 --et data/monthly_et_short_crop/full_monthly_et_short_crop.nc --et_var et_short_crop --sm data/soil_moisture/full_soil_moisture.nc --sm_var sm_pct --output cdi.nc 
    ```
    Where `{spi_3 file}` is the path to the netCDF file for SPI-3 that we created in step 3.
    
    If you experience problems with this step due to a high volume of data, try running on a HPC or using the 
    `--multiprocessing single` option.
    
    A file should be created called `cdi.nc` containing the result.
    
5. (Optional) Generate some maps
    
    You could view the result in a netCDF viewer such as [Panoply](https://www.giss.nasa.gov/tools/panoply/download/)
    or [ArcGis](https://www.arcgis.com/index.html), or you can generate some maps from the data.
    
    ```
    python generate_maps.py --netcdf cdi.nc --var_name cdi --output_file_base maps/CDI-1/CDI-1_ --title "Australian Combined Drought Indicator" --subtitle "last 1 month" --colourbar_label CDI --levels 0 0.02 0.05 0.1 0.2 0.3 0.7 0.8 0.9 0.95 0.98 100 --colours \#612804 \#b94402 \#f47b13 \#fdce72 \#fff1b4 \#ffffff \#cddcee \#98c8e1 \#3d8bbc \#0d569c \#082f71 --categories "Exceptional drought, Extreme drought, Severe drought, Moderate drought, Slightly dry, Near Normal, Slightly wet, Moderate wet, Severe wet, Extreme wet, Exceptional wet" --no_data
    ```
    
    This will create one map for every month in `cdi.nc`, which can be found in the `maps/CDI` directory. If you only 
    want to generate a few maps, you can use the `--start_date` and `--end_date` options. Note that you need to use the 
    -o option if you want to overwrite maps which have already been created.
 
## Scripts

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
| --path             | Determines where the data will be saved. Defaults to data/{dataset}/{date}.{dataset}.{filetype}                                                                                                        |
| --datasets         | Which datasets to download. This argument is required. Accepts multiple arguments. Check DATASET_CHOICES inside script to see options for this argument.                                                                                                                    |
| -v, --verbose      | Increase output verbosity |

If you cancel this script midway through it's likely that the most recent file will be empty/corrupt so you should 
delete it before running again.

### prep_files.py

General pre-processing that is necessary before downloaded data can be used in the 
[Climate Indices](https://github.com/monocongo/climate_indices) package or for calculating the CDI. Datasets are 
standardized to the following criteria:

* Extracting data for Australia from global datasets
* Regridding to a spatial resolution of 0.05 degrees (~5km)
* Reordering dimensions into: latitude, longitude, time (this is necessary because the 
  [Climate Indices](https://github.com/monocongo/climate_indices) package will only accept datasets with dimensions in 
  this order)
* Aggregating daily datasets to monthly
* Combining recently downloaded data with previously downloaded data (in the case of soil moisture where complete model
  output is only available by email request)
* Calculating average temperature from minimum and maximum temperatures
* Creating additional datasets for soil moisture, rainfall, average temperature, and evapotranspiration that are 
  aggregated to 3, 6, 9, 12, 24 and 36 months

The resulting files can be found in {dataset}/full_{dataset}.nc, depending on the path you specify.

```
Usage: python download.py --path PATH --datasets dataset1 dataset2
```

Arguments:  

|||
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --path             | Determines where the input files can be found. Defaults to data/{dataset}/{year}.{dataset}.nc. Output will be saved in the same directory as 'full_{dataset}.nc'                                                                |
| --datasets         | Which datasets to prepare. This argument is required. Accepts multiple arguments. Check DOWNLOADED_DATASETS and COMPUTED_DATASETS inside script to see options for this argument.
| -v, --verbose      | Increase output verbosity |

### calculate_cdi.py

Script to calculate the Australian Combined Drought Indicator. Expects input of four netCDF files: Normalised 
Difference Vegetation Index (NDVI), Short Crop Evapotranspiration, 3-month SPI, and Rootzone Soil Moisture.

Data from all four inputs are required. In the event that one or more are missing for a coordinate, that coordinate will
be filled in with a NaN value. If one or more inputs are missing for all of Australia for certain dates, that date 
will be excluded from the result.

```
Usage: python calculate_cdi.py --ndvi NDVI_PATH --ndvi_var NDVI_VAR --spi SPI_PATH --spi_var SPI_VAR --et ET_PATH --et_var ET_VAR --sm SM_PATH --sm_var SM_VAR --output OUTPUT_FILE_PATH
```

Required arguments:

|||
|--------------------|-------------------------------------------------------------------------------------------------|
| --ndvi             | The path to the input NDVI netCDF file.                                                         |
| --ndvi_var         | The name of the NDVI variable in the netCDF file.                                               |
| --spi              | The path to the input SPI netCDF file.                                                          |
| --spi_var          | The name of the SPI variable in the netCDF file.                                                |
| --et               | The path to the input evapotranspiration netCDF file.                                           |
| --et_var           | The name of the evapotranspiration variable in the netCDF file.                                 |
| --sm               | The path to the input Soil Moisture netCDF file.                                                |
| --sm_var           | The name of the soil moisture variable in the netCDF file.                                      |
| --output           | Where to save the output file.                                                                  |

Optional arguments: 

|||
|--------------------|-----------------------------------------------------------------------------------------------------------|
| -v, --verbose      | Increase output verbosity                                                                                 |
| --multiprocessing  | Number of processes to use in multiprocessing. Options: single, all_but_one, all. Defaults to all_but_one.|
| --weights          | Path for the file containing custom weightings for the weighted average performed on input datasets.      |

### generate_maps.py

This script accepts a netCDF file and generates visual maps from that data. It generates one map per date. Maps will 
have a colour bar legend with a customisable colour bar label, main title, and subtitle. Maps will be saved in a 
directory defined by the `output_file_base` option. Areas with NaN values will be greyed out.

```
Usage: python generate_maps.py --netcdf NETCDF_PATH --var_name VAR_NAME --output_file_base OUTPUT_FILE_BASE
```

Required arguments:  

|||
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --netcdf           | The path of the netCDF file containing the data.                                                                                                            |
| --var_name         | The name of the variable to plot.                                                                                                                           |
| --output_file_base | Base file name for all output files. Each image file will begin with this base name plus the date of the time slice. (e.g. SPI-1 becomes SPI-1_1889-01.jpg) |

Optional arguments:

|||
|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| -o, --overwrite       | Existing images will be overwritten. Default behavior is to skip existing images.                                                                                                                                                                                              |
| --shape               | The path of an optional shape file to use for the base map, such as from [gadm.org](https://gadm.org/). If not provided, a default will be provided.                                                                                                                           |
| --start_date          | This tool will only produce maps for dates between start_date and end_date. Dates should be given in the format of 2017-08. If only one is provided, all maps before or after the date will be produced. If this option  isn't used, default behavior is to generate all maps. |
| --end_date            | See --start_date                                                                                                                                                                                                                                                               |
| --title               | Sets the map's title on the lower left. |
| --subtitle            | Sets the map's subtitle on the lower left |
| --label_position      | Sets the position of the title, subtitle and date on the image. Given as a fraction of the image from the bottom-left corner. Default: 0.1 0.05 |
| --colours             | A list of hex colours to use for the map, from lowest value to highest. There should be one more colour than there are levels. |
| --colourbar_label     | The label above the colourbar legend (usually an abbreviation of the index name) |
| --colourbar_position  | Sets the position of the colourbar. Given as a fraction of the image from the bottom-left corner. Default: 0.807 0.6 |
| --categories          | Labels to replace the numbered levels on the colourbar. |
| --min                 | The minimum level for the plotted variable shown in the map and colourbar. |
| --max                 | The maximum level for the plotted variable shown in the map and colourbar. |
| --levels              | If one number is given, it is the number of levels for the plotted variable shown in the map and colorbar. If multiple numbers are given, they will be used as a list to explicitly set each level. Example: 8, or 0 1 2 3 4 5 6 7 8 |
| --region              | Mask out all of the states except the one specified by this argument (e.g. "Queensland" or '"Northern Territory") |
| --extent              | Defines the extent of the map in latitude and longitude. Should be given as four values: left, right, bottom and top. Example: 137, 155, -10, -30 |
| -p, --prototype       | Adds an overlay to the image labelling it as a prototype. |
| --no-data             | Adds a No Data section to the colourbar legend. Use this if blank areas are common on this type of map. |
| -v, --verbose         | Increase output verbosity |
| --multiprocessing     | Number of processes to use in multiprocessing. |
| --no_downsampling     | Don't downsample data to generate maps faster. This option can be helpful if downsampling is causing blurred borders. |
| --time_window         | The number of months that this map is portraying. (e.g. 3) |
| --time_window_type    | Used to determine whether the date of a map is the beginning or the end of the time window, when the --time_window option is used. Options: beginning, end. |

### percentile_rank.py

Percentile ranks data for a netCDF file. Designed for climate data with time, longitude and latitude dimensions. Each 
point on the grid is percentile ranked relative to the historical conditions on that month (e.g. 2001 January is 
ranked against all other Januaries).

|||
|---------------|-----------------------------------------------------------------------------------------|
| --input       | The path of the file to use as input (required)                                         |
| --output      | The location to save the result. If not supplied, the input file will be overwritten.   |
| --vars        | The variables in the netCDF file to percentile rank. Will rank all variables by default |
| -v, --verbose | Increase output verbosity                                                               |

## Contacts

**Laura Guillory**  
_Web Developer_  
Centre for Applied Climate Science  
University of Southern Queensland  
[laura.guillory@usq.edu.au](mailto:laura.guillory@usq.edu.au)

**Jillian Jackson**
_Technician_
Centre for Applied Climate Science
University of Southern Queensland
[jillian.jackson@usq.edu.au](mailto:jillian.jackson@usq.edu.au)
