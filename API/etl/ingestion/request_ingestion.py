import os
import requests
import argparse
from datetime import datetime
from timeit import default_timer as timer

raw_path = "/mnt/datalake/raw/imdb"

base_request_path = "https://datasets.imdbws.com/"

data_extension = ".tsv.gz"

def data_asset_ingestion(data_asset):
    
    file_folder_path = os.path.join(raw_path, data_asset.replace(".", "_"), datetime.now().strftime("%Y-%m-%d"))
    if not os.path.exists(file_folder_path):
        os.makedirs(file_folder_path)
    
    file_path = os.path.join(file_folder_path, data_asset+data_extension)
    
    with open(file_path, "wb") as f:
        with requests.Session() as s:
            f.write(s.get(base_request_path+data_asset+data_extension).content)
            
    return file_folder_path


def main():
    
    start = timer()
    
    parser = argparse.ArgumentParser(description="Ingestion Process")
    
    parser.add_argument('--data_asset', type=str, dest="data_asset")
    
    args = parser.parse_args()
    data_asset = args.data_asset
    print(f"Data asset: {data_asset}")
    
    if not os.path.exists(raw_path):
        os.makedirs(raw_path)
    
    file_path = data_asset_ingestion(data_asset)
        
    end = timer()
    print(end - start)
    
    print(file_path)
    

if __name__ == "__main__":
    main()