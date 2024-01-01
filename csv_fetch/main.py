import requests
from tqdm import tqdm

url = "path/to/static/file.csv"       # 'path/to/static/CSV/file.csv' is a temporary name. Change it to match the file's actual URL
response = requests.get(url, stream=True)

with open("file_name.csv", "wb") as file, tqdm(        # 'file_name.csv' is a temporary name. Change it to match the actual file name
        desc="Downloading File",
        total=int(response.headers.get('content-length', 0)),
        unit='iB',
        unit_scale=True
    ) as bar:
    for data in response.iter_content(chunk_size=1024):
        size = file.write(data)
        bar.update(size)
