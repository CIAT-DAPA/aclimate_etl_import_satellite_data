import argparse

from tools import Tools
from chirps_data import ChirpsData
from era5_data import Era5Data
from geoserver_upload import UploadGeoserver


def main():

    try:

        parser = argparse.ArgumentParser(description="Download satellite data packages")

        parser.add_argument("-o", "--outputs", help="Outputs path", required=True)
        parser.add_argument("-s", "--startDate", help="Start date to download example: 2024-07", required=True)
        parser.add_argument("-e", "--endDate", help="End date to download example: 2024-09", required=True)
        parser.add_argument("-c", "--country", help="Country", required=True)
        parser.add_argument("-d", "--download", help="Download data path", required=True)
        parser.add_argument("-w", "--workspace", help="Geoserver workspace", required=True)


        args = parser.parse_args()

        print("Reading inputs")
        print(args)

        output_path = args.outputs

        download_path = args.download

        start_date = args.startDate

        end_date = args.endDate
        
        country = args.country

        workspace = args.workspace

        tools = Tools()
        tools.validate_dates(start_date, end_date)

        cd = ChirpsData(output_path, country, start_date, end_date, download_path)
        cd.main()

        e5 = Era5Data(output_path, country, start_date, end_date, download_path)
        e5.main()

        geo = UploadGeoserver(output_path, country, start_date, end_date, workspace)
        geo.main()
    except ValueError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()