import requests
from tqdm import tqdm

url = "https://data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv?date=20231229&accessType=DOWNLOAD"
response = requests.get(url, stream=True)

with open("service_requests.csv", "wb") as file, tqdm(
        desc="Downloading 311 Data",
        total=int(response.headers.get('content-length', 0)),
        unit='iB',
        unit_scale=True
    ) as bar:
    for data in response.iter_content(chunk_size=1024):
        size = file.write(data)
        bar.update(size)
