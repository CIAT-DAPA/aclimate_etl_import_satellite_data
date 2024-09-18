import argparse

from tools import Tools
from chirps_data import ChirpsData
from era5_data import Era5Data


def main():

    parser = argparse.ArgumentParser(description="Download satellite data packages")

    parser.add_argument("-o", "--outputs", help="Outputs path", required=True)
    parser.add_argument("-s", "--startDate", help="Start date to download example: 2024-07", required=True)
    parser.add_argument("-e", "--endDate", help="End date to download example: 2024-09", required=True)
    parser.add_argument("-c", "--country", help="Country", required=True)
    parser.add_argument("-w", "--worskpace", help="Geoserver worskpace", required=True)


    args = parser.parse_args()

    print("Reading inputs")
    print(args)

    output_path = args.outputs

    start_date = args.startDate

    end_date = args.endDate
    
    country = args.country

    cd = ChirpsData(output_path, country, start_date, end_date)
    cd.main()

    e5 = Era5Data(output_path, country, start_date, end_date)
    e5.main()


if __name__ == "__main__":
    main()