# Main script to run the GeoJSON data fetching and processing

import time
import logging
from geojson_etl.scripts.etl.geodata_processing import fetch_and_insert_geodata
from db_config import db_credentials


def main():
    """Main function to execute the script."""
    start_time = time.time()
    geojson_url = "https://data.cityofnewyork.us/resource/3q43-55fe.geojson"
    page_size = 1000
    delay = 1

    total_records_fetched = fetch_and_insert_geodata(geojson_url, page_size, delay, db_credentials)
    logging.info(f"Total records fetched and inserted: {total_records_fetched}")

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"The script took {duration:.2f} seconds to run.")


if __name__ == "__main__":
    main()