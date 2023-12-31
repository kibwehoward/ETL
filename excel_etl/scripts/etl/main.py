import time
import logging
import requests
import pandas as pd
import warnings
from sqlalchemy import create_engine
from db_config import db_credentials
from io import BytesIO

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)


def download_excel(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content


def process_and_insert_data(excel_data, db_creds):
    u, p, h, port, db = db_creds.values()
    table_name = "excel_etl"

    excel_data_io = BytesIO(excel_data)

    with create_engine(f"postgresql://{u}:{p}@{h}:{port}/{db}").connect() as connection:
        df = pd.read_excel(excel_data_io, engine='openpyxl')
        df.to_sql(table_name, connection, if_exists='append', index=False, chunksize=500)


def main():
    start_time = time.time()
    excel_url = "https://www3.epa.gov/pesticides/appril/apprildatadump_public.xlsx"

    logging.info("Downloading Excel file...")
    excel_data = download_excel(excel_url)

    logging.info("Processing and inserting data...")
    process_and_insert_data(excel_data, db_credentials)

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"The script took {duration:.2f} seconds to run.")


if __name__ == "__main__":
    main()
