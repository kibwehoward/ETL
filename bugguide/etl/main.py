import time
import logging
import requests
from sqlalchemy import create_engine
from db_config import db_credentials
from io import BytesIO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

def download_data(url):
    """Download data from a given URL."""
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def concatenate_data(urls):
    """Concatenate data from a list of URLs."""
    combined_data = b""
    for url in urls:
        logging.info(f"Downloading data from {url}")
        combined_data += download_data(url)
    return combined_data

def insert_data(data, db_creds):
    """Insert data into a database."""
    u, p, h, port, db = db_creds.values()
    table_name = "bugguide"
    data_io = BytesIO(data)
    with create_engine(f"postgresql://{u}:{p}@{h}:{port}/{db}").connect() as connection:
        connection.execute(f"COPY {table_name} FROM STDIN", data_io)

def main():
    """Main function to download, concatenate, and insert data."""
    start_time = time.time()

    # List of API URLs
    api_urls = [
        "https://api.bugguide.net/taxonomy/Blattodea.txt"
        "https://api.bugguide.net/taxonomy/Coleoptera.txt"
        "https://api.bugguide.net/taxonomy/Dermaptera.txt"
        "https://api.bugguide.net/taxonomy/Diptera.txt"
        "https://api.bugguide.net/taxonomy/Embiidina.txt"
        "https://api.bugguide.net/taxonomy/Ephemeroptera.txt"
        "https://api.bugguide.net/taxonomy/Hemiptera.txt"
        "https://api.bugguide.net/taxonomy/Hymenoptera.txt"
        "https://api.bugguide.net/taxonomy/Lepidoptera.txt"
        "https://api.bugguide.net/taxonomy/Mantodea.txt"
        "https://api.bugguide.net/taxonomy/Mecoptera.txt"
        "https://api.bugguide.net/taxonomy/Megaloptera.txt"
        "https://api.bugguide.net/taxonomy/Microcoryphia.txt"
        "https://api.bugguide.net/taxonomy/Neuroptera.txt"
        "https://api.bugguide.net/taxonomy/Notoptera.txt"
        "https://api.bugguide.net/taxonomy/Odonata.txt"
        "https://api.bugguide.net/taxonomy/Orthoptera.txt"
        "https://api.bugguide.net/taxonomy/Phasmida.txt"
        "https://api.bugguide.net/taxonomy/Plecoptera.txt"
        "https://api.bugguide.net/taxonomy/Protorthoptera.txt"
        "https://api.bugguide.net/taxonomy/Psocodea.txt"
        "https://api.bugguide.net/taxonomy/Raphidioptera.txt"
        "https://api.bugguide.net/taxonomy/Siphonaptera.txt"
        "https://api.bugguide.net/taxonomy/Strepsiptera.txt"
        "https://api.bugguide.net/taxonomy/Thysanoptera.txt"
        "https://api.bugguide.net/taxonomy/Trichoptera.txt"
        "https://api.bugguide.net/taxonomy/Zoraptera.txt"
        "https://api.bugguide.net/taxonomy/Zygentoma.txt"
    ]

    # Concatenate data from all URLs
    concatenated_data = concatenate_data(api_urls)

    # Insert data into database
    logging.info("Inserting data into database...")
    insert_data(concatenated_data, db_credentials)

    end_time = time.time()
    logging.info(f"The script took {end_time - start_time:.2f} seconds to run.")

# Execute the main function
if __name__ == "__main__":
    main()
