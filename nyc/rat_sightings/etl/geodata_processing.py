# Module for processing and inserting GeoJSON data into a PostgreSQL database

import json
import os
import subprocess
import requests
import logging
import time
from sqlalchemy import create_engine
from sqlalchemy.sql import text  # Import the text function from sqlalchemy.sql

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../debug.log"),
        logging.StreamHandler()
    ]
)


def create_db_engine(credentials):
    """Create a database engine using provided credentials."""
    engine_url = (f'postgresql://{credentials["username"]}:'
                  f'{credentials["password"]}@{credentials["host"]}:'
                  f'{credentials["port"]}/{credentials["database"]}')
    return create_engine(engine_url)


def test_db_connection(db_creds):
    """Test the database connection."""
    engine = create_db_engine(db_creds)
    try:
        with engine.connect() as conn:
            from sqlalchemy.sql import text
            result = conn.execute(text("SELECT COUNT(*) FROM rat_sightings"))
            for row in result:
                print(row)
    except Exception as e:
        print(f"An error occurred: {e}")


def get_latest_date(db_creds):
    """Get the latest date of data entry in the database."""
    engine = create_db_engine(db_creds)
    try:
        with engine.connect() as connection:
            latest_date_query = text("SELECT MAX(created_date) FROM rat_sightings")
            result = connection.execute(latest_date_query)
            latest_date = result.scalar()
            return latest_date
    except Exception as e:
        logging.error(f"An error occurred while fetching the latest date: {e}")
        return None


def fetch_data(url, page_size, page, start_date=None):
    """Fetch GeoJSON data from the provided URL starting from the latest date."""
    params = {"$limit": page_size, "$offset": (page - 1) * page_size}
    if start_date:
        params["start_date"] = start_date  # Modify as per your API's parameter
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def process_and_insert_data(geojson_data, page, db_creds):
    """Process and insert GeoJSON data into a PostgreSQL database."""
    u, p, h, port, db = db_creds.values()

    temp_file_geojson = f"temp_data_{page}.geojson"
    with open(temp_file_geojson, 'w') as f:
        json.dump(geojson_data, f)

    temp_file_geopackage = f"temp_data_{page}.gpkg"
    cmd_convert = ['ogr2ogr', '-f', 'GPKG', temp_file_geopackage, temp_file_geojson]
    subprocess.run(cmd_convert, check=True)
    os.remove(temp_file_geojson)

    connection_string = (f"PG:host={h} user={u} dbname={db} "
                         f"password={p} port={port}")
    cmd_import = [
        'ogr2ogr',
        '-f', 'PostgreSQL',
        connection_string,
        '-nln', 'rat_sightings',
        '-append',
        temp_file_geopackage
    ]
    subprocess.run(cmd_import, check=True)
    os.remove(temp_file_geopackage)


def fetch_and_insert_geodata(geojson_url, page_size, delay, db_creds):
    """Fetch and insert GeoJSON data by pagination."""
    page = 1
    total_records_fetched = 0
    latest_date = get_latest_date(db_creds)

    while True:
        logging.info(f"Fetching page {page}...")
        try:
            geojson_data = fetch_data(geojson_url, page_size, page, latest_date)
            if not geojson_data['features']:
                logging.info("No more data to fetch. Exiting loop.")
                break

            process_and_insert_data(geojson_data, page, db_creds)

            total_records_fetched += len(geojson_data['features'])
            logging.info(
                f"Successfully fetched and inserted page {page}. "
                f"Total records so far: {total_records_fetched}"
            )

        except requests.RequestException as e:
            logging.error(f"Failed to fetch data: {e}")
            continue

        except subprocess.SubprocessError as e:
            logging.error(f"Error in subprocess: {e}")
            continue

        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            continue

        page += 1
        time.sleep(delay)

    return total_records_fetched


# Uncomment to test database connection
# test_db
