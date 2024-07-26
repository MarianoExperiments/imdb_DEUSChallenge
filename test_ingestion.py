import os
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

raw_path = "/home/drm/DRM/imdb_DEUSChallenge/raw"

data_source="imdb"

base_request_path = "https://datasets.imdbws.com/"
data_assets = [
    "name.basics",
    "title.akas",
    "title.basics",
    "title.crew",
    "title.episode",
    "title.principals",
    "title.ratings",]
data_extension = ".tsv.gz"


def data_asset_ingestion(data_asset):
    
    file_path=os.path.join(raw_path, data_source, data_asset.replace(".", "_"), datetime.now().strftime("%Y-%m-%d"))
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    
    print(file_path)
    with open(os.path.join(file_path, data_asset+data_extension), "wb") as f:
        with requests.Session() as s:
            f.write(s.get(base_request_path+data_asset+data_extension).content)


if __name__ == "__main__":
    
    data_source_path = os.path.join(raw_path, data_source)
    
    if not os.path.exists(data_source_path):
        os.makedirs(data_source_path)
        
    # data_asset_raw_path = map(lambda x: os.path.join(data_source_path, x+data_extension), data_assets)
    # for data in data_asset_raw_path:
    #     print(data)
        
    with ThreadPoolExecutor() as executer:
        executer.map(data_asset_ingestion, data_assets)