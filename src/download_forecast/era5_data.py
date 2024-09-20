import os
import urllib.request
import gzip
import geopandas as gpd
import rasterio
import numpy as np
import xarray as xr
import rioxarray 
import cdsapi
from zipfile import ZipFile
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from rasterio.mask import mask
from shapely.geometry import mapping
from tools import DownloadProgressBar, Tools
from tqdm import tqdm


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

    self.ERA5_FILE = "_C3S-glob-agric_AgERA5_"
    self.ERA5_FILE_TYPE = "_final-v1.1.nc"
    self.cdsapi_version = "1_1"
    self.enum_variables ={
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

    pass

  def generate_days(self):
    return [f"{day:02}" for day in range(1, 32)]

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
  
  def get_file_name(self, variable):
    if variable == "t_max":
      return "Temperature-Air-2m-Max-24h"
    elif variable == "t_min":
      return "Temperature-Air-2m-Min-24h"
    elif variable == "sol_rad":
      return "Solar-Radiation-Flux"

  def check_files_exist(self, variable, start_date, end_date, directory):
      # Separar año y mes de las fechas de inicio y fin
      start_year, start_month = map(int, start_date.split('-'))
      end_year, end_month = map(int, end_date.split('-'))

      # Recorrer los años en el rango
      for year in range(start_year, end_year + 1):
          # Obtener los meses para el año actual
          months = self.generate_month_range(year, start_year, start_month, end_year, end_month)
          
          # Recorrer los meses
          for month in months:
              days = self.generate_days()  # Array con los días '01' a '31'
              
              # Recorrer los días
              for day in days:
                  # Formato de la fecha en YYYYMMDD
                  date_str = f"{year}{month}{day}"
                  # Construir el nombre esperado del archivo
                  expected_filename = f"{variable}{self.ERA5_FILE}{date_str}{self.ERA5_FILE_TYPE}"
                  # Ruta completa del archivo
                  file_path = os.path.join(directory, expected_filename)
                  
                  # Verificar si el archivo existe
                  if not os.path.isfile(file_path):
                      print(f"Missing file: {expected_filename}")
                      return False  # Falta algún archivo
      return True  # Todos los archivos están presentes

  def download_era5_data(self, variables=["t_max","t_min","sol_rad"]):
    
    # Define the variables classes and their parameters for the CDSAPI

    start_year, start_month = map(int, self.start_date.split('-'))
    end_year, end_month = map(int, self.end_date.split('-'))

    # Process for each variable that should be downloaded
    for v in variables:
      print("\tProcesing",v)

      variable_path = os.path.join(self.era5_path, self.get_variable(v))

      days_array = self.generate_days()

      if not self.check_files_exist(self.get_file_name(v), self.start_date, self.end_date, variable_path):
        for year in range(start_year, end_year + 1):
          # Definir los meses a recorrer según si es el año inicial, intermedio o final
          months = self.generate_month_range(year, start_year, start_month, end_year, end_month)

          file = os.path.join(variable_path, f"{year}_{v}.zip")

          c = cdsapi.Client(timeout=600)
          c.retrieve('sis-agrometeorological-indicators',
              {
                  'format': 'zip',
                  'variable': self.enum_variables[v]["name"],
                  'statistic': self.enum_variables[v]["statistics"],
                  'year': year,
                  'month': months,
                  'day': days_array,
                  'version': self.cdsapi_version,
              },
              file
          )
          
          with ZipFile(file, 'r') as zObject:
            # Extracting all the members of the zip
            # into a specific location.
            zObject.extractall(path=variable_path)
            print("\tExtracted!")
          
          os.remove(file)
          print("\tZIP file removed:", file)

      else:
        print("\tFile already downloaded!")

  def netcdf_to_raster(self, save_path):
    """
    Convierte NetCDFs a archivos .tif en un rango de fechas y guarda los resultados en una carpeta temporal.

    :param save_path: Ruta base donde se guardarán los archivos .tif
    """

    variables=["t_max","t_min","sol_rad"]
    new_crs = '+proj=longlat +datum=WGS84 +no_defs'

    # Generar rango de fechas
    start_year, start_month = map(int, self.start_date.split('-'))
    end_year, end_month = map(int, self.end_date.split('-'))

    # Recorrer las variables
    for variable in variables:
      print(f"\nProcessing variable: {variable}")

      tmp_save_path = os.path.join(save_path, self.get_variable(variable), "tmp")
      self.tools.create_dir(tmp_save_path)

      # Ruta donde se almacenan los NetCDFs de la variable
      variable_path = os.path.join(self.era5_path, self.get_variable(variable))
      
      for year in range(start_year, end_year + 1):
        months = self.generate_month_range(year, start_year, start_month, end_year, end_month)

        # Recorrer los meses
        for month in months:
            days_array = self.generate_days()

            # Recorrer cada día del mes
            for day in days_array:
              # Construir el nombre del archivo NetCDF a partir del año, mes y día
              nc_file_name = f"{self.get_file_name(variable)}{self.ERA5_FILE}{year}{month}{day}{self.ERA5_FILE_TYPE}"
              input_file = os.path.join(variable_path, nc_file_name)

              if os.path.exists(input_file):
                print(f"\tConverting {input_file} to raster...")

                # Definir el archivo de salida .tif
                output_file = os.path.join(tmp_save_path, f"{self.get_variable(variable)}_{year}{month}{day}.tif")

                # Leer y procesar el archivo NetCDF
                xds = xr.open_dataset(input_file)
                
                # Transformación basada en la variable (según tu lógica)
                if self.enum_variables[variable]["transform"] == "-":
                    xds = xds - self.enum_variables[variable]["value"]
                elif self.enum_variables[variable]["transform"] == "/":
                    xds = xds / self.enum_variables[variable]["value"]

                # Aplicar CRS y guardar como raster
                xds.rio.write_crs(new_crs, inplace=True)
                variable_names = list(xds.variables)

                # Guardar en formato .tif
                xds[variable_names[3]].rio.to_raster(output_file)
                print(f"\tSaved raster to {output_file}")

              else:
                print(f"\tFile not found: {input_file}")
  
    print("\nConversion complete!")

  def cut_rasters(self, save_path):
    """
    Recorta los rasters de t_max, t_min y sol_rad utilizando un shapefile de país, 
    y guarda los resultados en la carpeta de salida correspondiente.

    :param save_path: Ruta base donde se encuentran los archivos .tif
    """
    # Abrir el shapefile de país
    shapefile = gpd.read_file(self.country_path)
    # Convertir el shapefile a una lista de geometrías
    shapes = [mapping(geom) for geom in shapefile.geometry]

    # Variables para procesar
    variables = ["t_max", "t_min", "sol_rad"]

    for variable in variables:
        print(f"\nProcessing variable: {variable}")

        # Ruta de los rasters .tif para la variable en la carpeta tmp
        tmp_rasters_path = os.path.join(save_path, self.get_variable(variable), "tmp")

        # Ruta de salida final para los rasters recortados (fuera de tmp)
        output_rasters_path = os.path.join(save_path, self.get_variable(variable))

        # Crear directorio de salida si no existe
        self.tools.create_dir(output_rasters_path)

        # Recorrer los archivos .tif en la carpeta tmp
        raster_files = [f for f in os.listdir(tmp_rasters_path) if f.endswith('.tif')]

        for raster_file in tqdm(raster_files, desc=f"Cutting rasters for {variable}"):
            raster_path = os.path.join(tmp_rasters_path, raster_file)

            # Abrir el raster
            with rasterio.open(raster_path) as src:

                # Verificar si el CRS del shapefile es diferente al del raster y reproyectar si es necesario
                if shapefile.crs != src.crs:
                    shapefile = shapefile.to_crs(src.crs)
                
                # Recortar el raster con el shapefile
                out_image, out_transform = mask(src, shapes, crop=True)
                if src.nodata is not None:
                    nodata = src.nodata
                    out_image[out_image == nodata] = np.nan

                # Manejo de valores no válidos
                invalid_value = -9999
                invalid_mask = (out_image == invalid_value)
                if np.any(invalid_mask):
                    print("Hay valores -9999 en los datos recortados. Reemplazando con np.nan.")
                    out_image[invalid_mask] = np.nan

                # Copiar metadatos del raster original y actualizar con el nuevo tamaño y transformación
                out_meta = src.meta.copy()
                out_meta.update({
                    "driver": "GTiff",
                    "height": out_image.shape[1],
                    "width": out_image.shape[2],
                    "transform": out_transform
                })

                # Definir el archivo de salida recortado en la carpeta de salida (no tmp)
                raster_cut_path = os.path.join(output_rasters_path, f"{raster_file}")

                # Guardar el raster recortado en la carpeta de salida
                with rasterio.open(raster_cut_path, "w", **out_meta) as dest:
                    dest.write(out_image)
                
                print(f"\tSaved cut raster to {raster_cut_path}")
        
        # Eliminar la carpeta tmp después de procesar todos los rasters
        if os.path.exists(tmp_rasters_path):
          # Primero, eliminar todos los archivos en tmp
          for file in os.listdir(tmp_rasters_path):
            file_path = os.path.join(tmp_rasters_path, file)
            os.remove(file_path)

          # Luego, eliminar el directorio tmp
          os.rmdir(tmp_rasters_path)
          print(f"\tDeleted temporary folder: {tmp_rasters_path}")
  
    print("\nAll rasters cut and saved in the output directories!")

  def main(self):
      
    self.download_era5_data()
    self.netcdf_to_raster(self.output_path)
    self.cut_rasters(self.output_path)
