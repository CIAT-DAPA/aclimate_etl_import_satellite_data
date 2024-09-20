# Satellite Data Download and Geoserver Upload

The main goal of this project is to facilitate the process of downloading satellite data packages and uploading them to a Geoserver instance. This tool allows users to easily retrieve data from sources like CHIRPS and ERA5, process it, and then publish the data in a Geoserver workspace for visualization and analysis.

# Satellite Data ETL

This component extracts data from external satellite sources (CHIRPS and ERA5), processes it, and uploads the processed data to a Geoserver workspace. Its primary function is to streamline the download, validation, and publishing of satellite data.

## Features

- Built using Python
- Supports data download from CHIRPS and ERA5
- Automates Geoserver upload of processed data
- Supports Python >= 3.10

### Prerequisites

- Python >= 3.10
- Geoserver credentials and URL

### Project Structure

- `downloadedData/:` Directory for storing downloaded raw data.
- `output/:` Folder where processed data is saved.
- `shapefiles/:` Contains shapefiles for different regions. For example, NICARAGUA/ includes Nicaragua shapefiles (.shp, .dbf, .prj, etc.).
- `src/:` Main source code folder.
- `src/geoserver_conexion/:` Handles connections to Geoserver.

# Instalation

To use ETL we must install a set of requirements, which are in a text file, for this process we recommend to create a virtual environment, this in order not to install these requirements in the entire operating system.

1. Clone the repository

```sh
git https://github.com/CIAT-DAPA/aclimate_etl_import_satellite_data
```

2. Create a virtual environment

```sh
python -m venv venv
```

3. Activate the virtual environment

- Linux

```sh
source env/bin/activate
```

- windows

```sh
env\Scripts\activate.bat
```

4. Install the required packages

```sh
pip install -r requirements.txt
```

### Required Environment Variables

Make sure to set up the following environment variables to connect with your Geoserver instance:

- GEO_USER: Your Geoserver username.
- EO_PASS: Your Geoserver password.
- GEO_URL: The URL of your Geoserver instance.

### Execution

Use the `main.py` script located inside the src/download_process/ directory to start the data download and upload process.

Command-line Parameters
The script accepts several command-line arguments to control the process:

`-o` or `--outputs`: Path where processed data will be saved.
`-s` or `--startDate`: Start date for downloading data, formatted as YYYY-MM.
`-e` or `--endDate`: End date for downloading data, formatted as YYYY-MM.
`-c` or `--country`: Country for which the data is being cut.
`-d` or `--download`: Path where raw data will be downloaded.
`-w` or `--workspace`: Geoserver workspace where the data will be uploaded.

## Usage examples

To download data for Nicaragua and upload it to the Geoserver:

```bash
python src/download_process/main.py -o output/ -s 2024-07 -e 2024-09 -c NICARAGUA -d downloadedData/ -w <GEOSERVER_WORKSPACE>
```
