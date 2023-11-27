import time
import logging
import requests
import pandas as pd
import json
from sqlalchemy import create_engine
from db_config import db_credentials

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)


def fetch_data_from_api(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()  # Assuming the API returns JSON data


def process_and_insert_data(api_data, db_creds):
    u, p, h, port, db = db_creds.values()
    table_name = "bugguide"

    # Convert the JSON data to a Pandas DataFrame
    df = pd.DataFrame(api_data)

    # Connecting to the database and inserting the data
    with create_engine(f"postgresql://{u}:{p}@{h}:{port}/{db}").connect() as connection:
        df.to_sql(table_name, connection, if_exists='append', index=False, chunksize=500)


def main():
    start_time = time.time()
    api_url = "https://bugs.verfasor.com/api"

    logging.info("Fetching data from API...")
    api_data = fetch_data_from_api(api_url)

    logging.info("Processing and inserting data...")
    process_and_insert_data(api_data, db_credentials)

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"The script took {duration:.2f} seconds to run.")


if __name__ == "__main__":
    main()
