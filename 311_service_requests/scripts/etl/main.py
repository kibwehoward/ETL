import requests
from tqdm import tqdm
import logging
from sqlalchemy import create_engine
import pandas as pd
from db_config import db_credentials
import os

# Configure logging
logging.basicConfig(filename='311_data_import.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# URL of the 311 Service Requests CSV
url = "https://data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv?date=20231229&accessType=DOWNLOAD"

# File path for the downloaded CSV
csv_file = "311_service_requests.csv"

# Define the batch size for processing
batch_size = 10000  # Adjust this based on your system's memory capacity

try:
    # Download the CSV file
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Check for HTTP errors

    total_size_in_bytes = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 Kibibyte

    with open(csv_file, "wb") as file, tqdm(
            desc="Downloading 311 Data",
            total=total_size_in_bytes,
            unit='iB',
            unit_scale=True) as bar:
        for data in response.iter_content(block_size):
            file.write(data)
            bar.update(len(data))

    logging.info("CSV download completed successfully.")

    # Create a database engine using db_credentials
    engine = create_engine(f'postgresql://{db_credentials["username"]}:{db_credentials["password"]}'
                           f'@{db_credentials["host"]}:{db_credentials["port"]}/{db_credentials["database"]}')

    # Process and load the CSV into PostgreSQL in batches
    for chunk in pd.read_csv(csv_file, chunksize=batch_size):
        chunk.to_sql(name='311_service_requests', con=engine, if_exists='append', index=False)
        logging.info("A batch of data loaded into the database successfully.")

    logging.info(f"Total records fetched and inserted: {batch_size}")

except requests.exceptions.RequestException as err:
    logging.error(f"Error in downloading CSV: {err}")
except Exception as err:
    logging.error(f"Error in processing data: {err}")
finally:
    # Clean up: Remove the CSV file
    if os.path.exists(csv_file):
        os.remove(csv_file)
        logging.info("Temporary CSV file deleted.")
    else:
        logging.error("The temporary file does not exist or could not be deleted.")
