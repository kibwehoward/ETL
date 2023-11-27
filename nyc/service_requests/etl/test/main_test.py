# Main script to run the GeoJSON data fetching and processing

import time
import logging
from geodata_processing_test import (
    get_latest_date,
    fetch_data,
    process_and_insert_data,
    create_db_engine
)
from db_config import db_credentials

def main():
    """Main function to execute the script."""
    start_time = time.time()

    # URL and other parameters
    geojson_url = "https://data.cityofnewyork.us/resource/3q43-55fe.geojson"
    page_size = 1000
    page = 1

    # Fetch the latest date from the database
    latest_date = get_latest_date(db_credentials)
    if latest_date is None:
        logging.error("Failed to fetch the latest date from the database.")
        return

    # Fetch GeoJSON data starting from the latest date
    geojson_data = fetch_data(geojson_url, page_size, page, latest_date)

    # Process and insert the data into the database
    process_and_insert_data(geojson_data, db_credentials)

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"The script took {duration:.2f} seconds to run.")

if __name__ == "__main__":
    main()
