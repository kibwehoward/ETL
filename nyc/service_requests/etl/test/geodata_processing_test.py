# Module for processing and inserting GeoJSON data into a PostgreSQL database

import requests
import logging
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime
import pytz  # Ensure pytz is installed

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


def get_latest_date(db_creds):
    """Get the latest date of data entry in the database."""
    engine = create_db_engine(db_creds)
    try:
        with engine.connect() as connection:
            latest_date_query = text("SELECT MAX(created_date) FROM rat_sightings")
            result = connection.execute(latest_date_query)
            latest_date = result.scalar()

            # Format the date to match the API's expected format
            if latest_date:
                # Assuming the date is timezone-aware, adjust to New York timezone
                latest_date = latest_date.astimezone(pytz.timezone('America/New_York'))
                return latest_date.strftime("%Y-%m-%dT%H:%M:%S.000")
            else:
                return None
    except Exception as e:
        logging.error(f"An error occurred while fetching the latest date: {e}")
        return None


def fetch_data(url, page_size, page, start_date=None):
    """Fetch GeoJSON data from the provided URL starting from the latest date."""
    params = {"$limit": page_size, "$offset": (page - 1) * page_size}
    if start_date:
        params["start_date"] = start_date
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def process_and_insert_data(geojson_data, db_creds):
    """Process and insert the fetched GeoJSON data into the database."""
    processed_data = process_geojson(geojson_data)
    insert_into_database(processed_data, db_creds)


def process_geojson(geojson_data):
    """Process the GeoJSON data."""
    # Implement your data processing logic here
    # Example: extracting relevant fields, data transformation, etc.
    processed_data = []  # Placeholder for processed data
    # Process each feature in the GeoJSON data
    for feature in geojson_data['features']:
        # Extract and transform data as needed
        processed_data.append(feature)  # Modify as per your needs
    return processed_data


def insert_into_database(data, db_creds):
    """Insert processed data into the database."""
    engine = create_db_engine(db_creds)
    with engine.connect() as connection:
        for record in data:
            # Construct and execute your INSERT statements here
            # Example: connection.execute(your_insert_statement)
            pass  # Replace with your database insertion logic

# Example usage (typically called from a main script)
# db_creds = {...}  # Database credentials
# url = "https://data.cityofnewyork.us/resource/3q43-55fe.geojson"
# page_size = 1000
# page = 1
# latest_date = get_latest_date(db_creds)
# geojson_data = fetch_data(url, page_size, page, latest_date)
# process_and_insert_data(geojson_data, db_creds)
