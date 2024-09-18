import os
import urllib.request
import gzip
import geopandas as gpd
import rasterio
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from rasterio.mask import mask
from shapely.geometry import mapping
from tools import DownloadProgressBar, Tools


class ChirpsData():

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
    self.chirps_output_path = os.path.join(self.output_path,"PREC")

    self.downloaded_data_path = os.path.join(self.project_root,"downloadedData")

    self.chirps_path = os.path.join(self.downloaded_data_path,"CHIRPS")

    self.prec_path = os.path.join(self.chirps_path,"PREC")


    self.tools.create_dir(self.downloaded_data_path)
    self.tools.create_dir(self.chirps_path)
    self.tools.create_dir(self.prec_path)
    self.tools.create_dir(self.chirps_output_path)

    self.CHIRPS_URL = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05/year"
    self.CHIRPS_FILE = "chirps-v2.0.date.tif.gz"

    pass

  
  def download_file(self, url, path, remove = True):
    if os.path.exists(path.replace('.gz','')) == False:
        if os.path.exists(path.replace('.gz','')):
          os.remove(path.replace('.gz',''))
        with DownloadProgressBar(unit='B', unit_scale=True,miniters=1, desc=url.split('/')[-1]) as t:
          urllib.request.urlretrieve(url, filename=path, reporthook=t.update_to)
        with gzip.open(path, 'rb') as f_in:
          with open(path.replace('.gz',''), 'wb') as f_out:
          # Read the compressed content and write it to the output file
            f_out.write(f_in.read())
        os.remove(path)
    else:
        print("\tFile already downloaded!",path)

  def downloadData(self):
      
    dates = self.tools.generate_dates(self.start_date, self.end_date)

    urls = [f"{self.CHIRPS_URL.replace('year', date.split('-')[0])}/{self.CHIRPS_FILE.replace('date',date.replace('-','.'))}" for date in dates]
    files = [os.path.basename(url) for url in urls]
    save_path_chirp_all = [os.path.join(self.prec_path, file) for file in files]

    # Download in parallel
    with ThreadPoolExecutor(max_workers=self.cores) as executor:
      executor.map(self.download_file, urls, save_path_chirp_all)

    return save_path_chirp_all
      
  def cutRasters(self, rasters_path):

    # Abrir el shapefile
    shapefile = gpd.read_file(self.country_path)
    # Convertir el shapefile a una lista de geometrías
    shapes = [feature["geometry"] for feature in shapefile.__geo_interface__["features"]]
    # Convertir el shapefile a una lista de geometrías
    shapes = [mapping(geom) for geom in shapefile.geometry]

    for raster in rasters_path:

      raster_path = raster.replace(".gz", "")

      # Abrir el raster
      with rasterio.open(raster_path) as src:

        if shapefile.crs != src.crs:
          shapefile = shapefile.to_crs(src.crs)
        
        # Recortar el raster con el shapefile
        out_image, out_transform = mask(src, shapes, crop=True)
        if src.nodata is not None:
          nodata = src.nodata
          out_image[out_image == nodata] = np.nan

        invalid_value = -9999
        invalid_mask = (out_image == invalid_value)
        if np.any(invalid_mask):
          print("Hay valores -9999 en los datos recortados. Reemplazando con np.nan.")
          out_image[invalid_mask] = np.nan
 
        out_meta = src.meta.copy()

        # Actualizar los metadatos del raster recortado
        out_meta.update({
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform
        })

        # Guardar el raster recortado

        raster_cut = os.path.join(self.chirps_output_path, os.path.basename(raster_path).replace("chirps-v2.0.", "PREC_"))
        name, extension = os.path.basename(raster_cut).rsplit(".", 1)
        name = name.replace(".", "")
        new_file_name = f"{name}.{extension}"

        with rasterio.open(raster_cut.replace(os.path.basename(raster_cut), new_file_name), "w", **out_meta) as dest:
          dest.write(out_image)



    
  def main(self):
      
    rasters_path = self.downloadData()
    self.cutRasters(rasters_path)