import os
import pandas as pd
import numpy as np
import rasterio
import calendar

class DataExtractor:
  def __init__(self, output_path, csv_path, start_date, end_date):
    self.output_path = output_path
    self.csv_path = csv_path
    self.start_date = start_date
    self.end_date = end_date

  def read_coordinates(self):
    # Leer el CSV con las coordenadas
    coords_df = pd.read_csv(self.csv_path)
    return coords_df

  def extract_raster_data(self, start_date, end_date, lon, lat):
    # Convertir las fechas en objetos de fecha para facilitar el manejo
    start_year = int(start_date.split('-')[0])
    start_month = int(start_date.split('-')[1])
    end_year = int(end_date.split('-')[0])
    end_month = int(end_date.split('-')[1])
    
    data = {
        'day': [],
        'month': [],
        'year': [],
        't_max': [],
        't_min': [],
        'prec': [],
        'sol_rad': []
    }

    # Iterar a través de los años y meses en el rango definido
    for year in range(start_year, end_year + 1):
      
      for month in range(1, 13):
        if year == start_year and month < start_month:
            continue  # Saltar meses anteriores al inicio
        if year == end_year and month > end_month:
            break  # Salir del bucle si se excede el final
        # Iterar a través de los días del mes
        num_days = calendar.monthrange(year, month)[1]
        for day in range(1, num_days + 1):
          try:
            # Crear el prefijo de fecha para el raster
            date_str = f"{year}{month:02d}{day:02d}"  # Formato YYYYMMDD

            # Definir las rutas de los rasters dentro de las carpetas correspondientes
            t_max_raster = os.path.join(self.output_path, "TMAX", f"TMAX_{date_str}.tif")
            t_min_raster = os.path.join(self.output_path, "TMIN", f"TMIN_{date_str}.tif")
            prec_raster = os.path.join(self.output_path, "PREC", f"PREC_{date_str}.tif")
            sol_rad_raster = os.path.join(self.output_path, "SRAD", f"SRAD_{date_str}.tif")
            
            # Extraer datos de los rasters
            for raster_path in [t_max_raster, t_min_raster, prec_raster, sol_rad_raster]:
              if os.path.exists(raster_path):  # Comprobar si el archivo existe
                with rasterio.open(raster_path) as src:
                  # Obtener la transformación y la matriz de datos
                  transform = src.transform
                  top_left_x, top_left_y = transform[2], transform[5]
                  pixel_size_x = transform[0]  # Tamaño de celda en la dirección X (longitud)
                  pixel_size_y = -transform[4]
                  row, col = ~transform * (lon, lat)
                  row = round((top_left_y - lat) / pixel_size_y)
                  col = round((lon - top_left_x) / pixel_size_x)
                  
                  # Leer el valor en la posición (row, col)
                  value = src.read(1, window=((row, row + 1), (col, col + 1)))
                  value = value[0][0] if value.size > 0 else np.nan
                  # Almacenar los datos en el diccionario
                  
                  if raster_path == t_max_raster:
                      data['t_max'].append(value)
                  elif raster_path == t_min_raster:
                      data['t_min'].append(value)
                  elif raster_path == prec_raster:
                      data['prec'].append(value)
                  elif raster_path == sol_rad_raster:
                      data['sol_rad'].append(value)
              else:
                 print("File not found: ", raster_path)
            # Agregar la fecha al conjunto de datos
            data['day'].append(day)
            data['month'].append(month)
            data['year'].append(year)

          except Exception as e:
            continue  # Saltar si el archivo no se encuentra o hay otro error

    return data

  def save_to_csv(self, id, data):
    # Crear un DataFrame y guardar como CSV
    df = pd.DataFrame(data)
    output_csv = os.path.join(self.output_path, f"{id}_daily.csv")
    df.to_csv(output_csv, index=False)
    print(f"Saved data for ID {id} to {output_csv}")

  def process(self):
    coords_df = self.read_coordinates()
    for index, row in coords_df.iterrows():
        id = row['id']
        lat = row['lat']
        lon = row['long']
        data = self.extract_raster_data(self.start_date, self.end_date, lon, lat)
        self.save_to_csv(id, data)