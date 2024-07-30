import argparse
import importlib
from timeit import default_timer as timer


def main():
    
    start = timer()
    
    parser = argparse.ArgumentParser(description="Clean Process")
    
    parser.add_argument('--data_asset', type=str, dest="data_asset")
    parser.add_argument('--file_path', type=str, dest="file_path")
    parser.add_argument('--operation', type=str, dest="operation")
    
    
    args = parser.parse_args()
    data_asset = args.data_asset
    file_path = args.file_path
    operation = args.operation
    print(f"Data asset: {data_asset}")
    print(f"File Path: {file_path}")
    print(f"Operation: {operation}")
    
    clean_asset_module = importlib.import_module(f"cleansing.imdb.data_assets.clean_{data_asset.replace('.', '_')}")
    clean_asset_class = getattr(clean_asset_module, f"{data_asset.title().replace('.', '')}Clean")(file_path, operation)
    clean_asset_class.run()
    
    end = timer()
    print(end - start)
    


if __name__ == "__main__":
    main()