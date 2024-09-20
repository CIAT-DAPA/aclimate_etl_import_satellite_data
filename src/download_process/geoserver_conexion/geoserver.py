import os
import sys
from glob import glob
from geoserver_conexion.tool import GeoserverClient


class GeoserverImport():

    def __init__(self, workspace, user, passw, geo_url):
        self.geo_url = f"{geo_url}rest/"
        self.user = user
        self.pwd = passw
        self.workspace = workspace
        self.folder_root = os.path.dirname(os.path.realpath(__file__))
        self.folder_properties = os.path.join(self.folder_root, "properties")
        self.folder_layers = os.path.join(self.folder_root, "layers")
        os.makedirs(self.folder_layers, exist_ok=True)
        self.zip_path = os.path.join(self.folder_root, "zip")
        os.makedirs(self.zip_path, exist_ok=True)
        self.folder_tmp = os.path.join(self.folder_root, "tmp")
        os.makedirs(self.folder_tmp, exist_ok=True)


    def connect_geoserver(self):

        stores_aclimate = [x.split(os.sep)[-1] for x in glob(os.path.join(self.folder_layers,"*"), recursive = True)]


        try:

            print("Connecting")
            geoclient = GeoserverClient(self.geo_url, self.user, self.pwd)
            geoclient.connect()
            geoclient.get_workspace(self.workspace)
            print("Connected")

            for current_store in stores_aclimate:
                print("Working with",current_store)

                current_layer = os.path.join(self.folder_layers, current_store)

                store_name = current_store
                store = geoclient.get_store(store_name)

                if not store:
                    print("Creating mosaic")
                    geoclient.create_mosaic(store_name, current_layer, self.folder_properties, self.folder_tmp, self.zip_path)
                else:
                    print("Updating mosaic")
                    geoclient.update_mosaic(store, current_layer, self.folder_properties, self.folder_tmp, self.zip_path)

            return True    
        except Exception as e:
            print(str(e))
            return False

    
    def get_geoserver_stores(self):
        print("Connecting")
        geoclient = GeoserverClient(self.geo_url, self.user, self.pwd)
        geoclient.connect()
        geoclient.get_workspace(self.workspace)

        stores = geoclient.get_stores()
        return stores