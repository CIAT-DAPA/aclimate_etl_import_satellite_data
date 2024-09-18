import os
import urllib.request
import gzip
import geopandas as gpd
import rasterio
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from rasterio.mask import mask
from shapely.geometry import mapping
from tools import DownloadProgressBar
import cdsapi


class Era5Data():

  def __init__(self, output_path, country, start_date, end_date):
     
    self.output_path = output_path
    self.country = country
    self.start_date = start_date
    self.end_date = end_date

    self.tools = Tools()
    self.cores = 4

    self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    self.shapefile_path = os.path.join(self.project_root,"shapefiles")
    self.country_path = os.path.join(self.shapefile_path, self.country)
    self.era5_tmax_output_path = os.path.join(self.output_path,"TMAX")
    self.era5_tmin_output_path = os.path.join(self.output_path,"TMIN")
    self.era5_srad_output_path = os.path.join(self.output_path,"SRAD")

    self.downloaded_data_path = os.path.join(self.project_root,"downloadedData")

    self.era5_path = os.path.join(self.downloaded_data_path,"ERA5")

    self.tmax_path = os.path.join(self.era5_path,"TMAX")
    self.tmin_path = os.path.join(self.era5_path,"TMIN")
    self.srad_path = os.path.join(self.era5_path,"SRAD")

    self.tmax_output_path = os.path.join(self.output_path,"TMAX")
    self.tmin_output_path = os.path.join(self.output_path,"TMIN")
    self.srad_output_path = os.path.join(self.output_path,"SRAD")

    self.tools.create_dir(self.downloaded_data_path)
    self.tools.create_dir(self.era5_path)
    self.tools.create_dir(self.tmax_path)
    self.tools.create_dir(self.tmin_path)
    self.tools.create_dir(self.srad_path)
    self.tools.create_dir(self.tmax_output_path)
    self.tools.create_dir(self.tmin_output_path)
    self.tools.create_dir(self.srad_output_path)

    self.ERA5_FILE = "chirps-v2.0.date.tif.gz"
    self.cdsapi_version = "1_1"

    pass


  # Función para generar el rango de meses con formato 'MM'
  def generate_month_range(self, year, start_year, start_month, end_year, end_month):
    if year == start_year and year == end_year:
        # Caso especial cuando el rango está en el mismo año
        return [f"{month:02}" for month in range(start_month, end_month + 1)]
    elif year == start_year:
        # Desde el mes de inicio hasta diciembre
        return [f"{month:02}" for month in range(start_month, 13)]
    elif year == end_year:
        # Desde enero hasta el mes final
        return [f"{month:02}" for month in range(1, end_month + 1)]
    else:
        # Recorrer todos los meses del año
        return [f"{month:02}" for month in range(1, 13)]

  def get_variable(self, variable):
    if variable == "t_max":
      return "TMAX"
    elif variable == "t_min":
      return "TMIN"
    elif variable == "sol_rad":
      return "SRAD"

  def download_era5_data(self, variables=["t_max","t_min","sol_rad"]):
    new_crs = '+proj=longlat +datum=WGS84 +no_defs'
    # Define the variables classes and their parameters for the CDSAPI
    enum_variables ={
                        "t_max":{"name":"2m_temperature",
                                "statistics":['24_hour_maximum'],
                                "transform":"-",
                                "value":273.15},
                        "t_min":{"name":"2m_temperature",
                                "statistics":['24_hour_minimum'],
                                "transform":"-",
                                "value":273.15},
                        "sol_rad":{"name":"solar_radiation_flux",
                                "statistics":[],
                                "transform":"/",
                                "value":1000000}
                    }

    start_year, start_month = map(int, self.start_date.split('-'))
    end_year, end_month = map(int, self.end_date.split('-'))

    # Process for each variable that should be downloaded
    for v in variables:
      print("\tProcesing",v)

      for year in range(start_year, end_year + 1):
        # Definir los meses a recorrer según si es el año inicial, intermedio o final
        months = generate_month_range(year, start_year, start_month, end_year, end_month)

        days_array = [f"{day:02}" for day in range(1, 32)]

        variable_path = os.path.join(self.era5_path, self.get_variable(v))

        if os.path.exists(variable_path) == False:
          c = cdsapi.Client(timeout=600,quiet=False,verify=False)
          c.retrieve('sis-agrometeorological-indicators',
              {
                  'format': 'zip',
                  'variable': enum_variables[v]["name"],
                  'statistic': enum_variables[v]["statistics"],
                  'year': year,
                  'month': months,
                  'day': days_array,
                  'version': self.cdsapi_version,
              },
              variable_path
          )
        else:
          print("\tFile already downloaded!",variable_path)

        # with ZipFile(save_path_era5, 'r') as zObject:
        #   # Extracting all the members of the zip
        #   # into a specific location.
        #   zObject.extractall(path=save_path_era5_data_tmp)
        #   print("\tExtracted!")
        # else:
        #   print("\tFiles already extracted!",save_path_era5_data_tmp)

        # if self.force or len(os.listdir(save_path_era5_data)) == 0:
        #   tmp_files = glob.glob(os.path.join(save_path_era5_data_tmp, '*'))
        #   print("\tSetting CRS",save_path_era5_data_tmp,len(tmp_files))
        #   for file in tqdm(tmp_files,desc="nc to raster and setting new CRS " + v):
        #     input_file = file
        #     output_file = os.path.join(save_path_era5_data,file.split(os.path.sep)[-1].replace(".nc",".tif"))

        #     xds = xr.open_dataset(input_file)
        #     if enum_variables[v]["transform"] == "-":
        #       xds = xds - enum_variables[v]["value"]
        #     elif enum_variables[v]["transform"] == "/":
        #       xds = xds / enum_variables[v]["value"]
        #     xds.rio.write_crs(new_crs, inplace=True)
        #     variable_names = list(xds.variables)
        #     xds[variable_names[3]].rio.to_raster(output_file)
        #   print("\tSetted!")
        # else:
        #   print("\tFiles already transformed!",save_path_era5_data)


  def main(self):
      
    self.download_era5_data()
