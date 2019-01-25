# DownloadClimateData

This repository contains scripts for downloading gridded Australian climate data from 
[LongPaddock's SILO](https://silo.longpaddock.qld.gov.au/). 
 
Specifically, all the data is taken from [here](https://s3-ap-southeast-2.amazonaws.com/silo-open-data/annual/index.html).
Data is in NetCDF format.

## How To Use

Install [Anaconda](https://www.anaconda.com/download/) to use this.

Scripts can be found in the `scripts` directory. Please be advised they require a ton of disk space and time.

### download.py

Run download.py to download all the data, which will be stored in the newly created `data` directory. Any previously 
downloaded files will be skipped, so you can run this again to update your copy of the data without redownloading 
anything but the current year. 

If you cancel this script midway through it's likely that the most recent file will
be empty/corrupt so you should delete it before running again.

### prep_files.py

Prepares files for use in calculations such as [climate indices](https://github.com/monocongo/climate_indices). This 
consolidates all the files for each year into a single one, and reorders the dimensions into (lat, lon, time). This 
takes 20 minutes for one dataset. If there's a faster way please let me know.

## Contacts

**Laura Guillory**  
_Web Developer_  
Centre for Applied Climate Science  
University of Southern Queensland  
[laura.guillory@usq.edu.au](mailto:laura.guillory@usq.edu.au)