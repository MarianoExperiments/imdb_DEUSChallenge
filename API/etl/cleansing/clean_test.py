from pyspark.sql import SparkSession
from timeit import default_timer as timer


def main(data_asset, file_path):
    start = timer()
    
    spark = SparkSession.builder \
        .appName("Python Spark SQL example") \
        .getOrCreate()
    
    print(f"Data asset: {data_asset}")
    print(f"file_path: {file_path}")
    
    df = spark.read.csv(file_path, header=True, sep="\t")
    
    df.show(3, vertical=True)
    
    end = timer()
    print(end - start)
    


if __name__ == "__main__":
    main("title.principals", "/mnt/datalake/raw/imdb/title_principals/2024-07-26/title.principals.tsv.gz")