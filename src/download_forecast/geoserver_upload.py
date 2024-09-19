import os
import urllib.request
import gzip
import sys
import re
import shutil
from pathlib import Path
import requests
import xml.etree.ElementTree as ET
from tools import DownloadProgressBar, Tools, Response
from geoserver_conexion.geoserver import GeoserverImport


class UploadGeoserver():

  def __init__(self, output_path, country, start_date, end_date, workspace):
     
    self.output_path = output_path
    self.country = country
    self.start_date = start_date
    self.end_date = end_date
    self.workspace = workspace

    self.geoserver_user = os.getenv('GEO_USER')
    self.geoserver_pass = os.getenv('GEO_PASS')
    self.geoserver_url = os.getenv('GEO_URL')

    self.tools = Tools()



  def get_dates_from_geoserver(self, layer):
    try:
        # Construir la URL
        url = f"{self.geoserver_url}{self.workspace}/wms?service=WMS&version=1.3.0&request=GetCapabilities"
        
        # Hacer la solicitud GET al servidor
        response = requests.get(url)
        response.raise_for_status()  # Lanza un error si la solicitud no fue exitosa
        
        # Analizar el XML de la respuesta
        xmlDoc = ET.fromstring(response.content)
        
        # Buscar todos los elementos 'Layer'
        layers = xmlDoc.findall(".//{http://www.opengis.net/wms}Layer")
        dates = []

        # Si hay más de un 'Layer', buscar el que coincide con el nombre de la capa
        if len(layers) > 1:
            for layer_elem in layers[1:]:
                layer_name_elem = layer_elem.find("{http://www.opengis.net/wms}Name")
                
                if layer_name_elem is not None and layer_name_elem.text == layer:
                    dimension_elem = layer_elem.find("{http://www.opengis.net/wms}Dimension")
                    
                    if dimension_elem is not None:
                        dimension = dimension_elem.text
                        time_intervals = dimension.split(",")
                        
                        # Procesar las fechas
                        dates = [date.split("T")[0] for date in time_intervals]
                    break
            return dates
        else:
            print(f"The workspace {self.workspace}, does not have the layer selected: {layer}")
            return []
    
    except requests.exceptions.RequestException as error:
        print(f"Error getting available rasters from workspace: {self.workspace}")
        print(error)
        return []

  def importGeoserver(self):
    try:
      root_path  = os.path.dirname(os.path.realpath(__file__))
      geoserver_path= os.path.join(root_path, "geoserver_conexion")
      layer_path= os.path.join(geoserver_path, "layers")
      zip_path= os.path.join(geoserver_path, "zip")
      tmp_path= os.path.join(geoserver_path, "tmp")
      self.tools.create_dir(tmp_path)
      self.tools.create_dir(zip_path)
      self.tools.create_dir(layer_path)
      
      self.tools.copy_contents(self.output_path, layer_path)
      geoserver = GeoserverImport(self.workspace, self.geoserver_user, self.geoserver_pass, self.geoserver_url)
      result = geoserver.connect_geoserver()
      for file in os.listdir(self.output_path):
        file_path = os.path.join(self.output_path, file)
        shutil.rmtree(file_path)
      shutil.rmtree(tmp_path)
      shutil.rmtree(zip_path)
      shutil.rmtree(layer_path)
      if not result:
          print("Error saving")
          return

      print("Rasters were saved successfully")
    except Exception as e:
      print(e)


  def remove_duplicates(self, obj):
    # Iterate over the keys (layers) in the object
    for layer, dates in obj.items():
      # Path to the subfolder corresponding to the layer
      layer_path = os.path.join(self.output_path, layer)
      
      # Check if the subfolder exists
      if not os.path.exists(layer_path):
          print(f"The folder {layer_path} does not exist.")
          continue
      
      # Get files inside the subfolder
      for file in os.listdir(layer_path):
        file_path = os.path.join(layer_path, file)

        # Use a regex to extract the date from the filename (assuming format PREFIX_YYYYMMDD.tif)
        match = re.match(r'.*_(\d{8})\.tif', file)

        if match:
            # Extract date in YYYY-MM-DD format
            file_date = match.group(1)  # 'YYYYMMDD'
            formatted_date = f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:]}"  # Convert to 'YYYY-MM-DD'

            # Check if the file date is in the object's list of dates
            if formatted_date in dates:
                # If the date is found in the object, delete the file
                print(f"Removing duplicate file: {file_path}")
                os.remove(file_path)
        else:
            print(f"The file {file} does not follow the expected format.")


  def main(self):

    layer_dates = {}

    if len(os.listdir(self.output_path)) > 0:
      for layer in os.listdir(self.output_path):
        layer_dates[layer] = self.get_dates_from_geoserver(layer)
      self.remove_duplicates(layer_dates)
      if self.tools.has_file(self.output_path):
        self.importGeoserver()
      else:
        print("All files are already on the geoserver")
    else:
      print("There is no data to import")