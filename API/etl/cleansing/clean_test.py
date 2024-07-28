import argparse
from pyspark.sql import SparkSession
from timeit import default_timer as timer
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

def start_session():
    spark = SparkSession.builder \
            .config("spark.jars", "/opt/spark_jars/postgresql-42.7.3.jar") \
                .getOrCreate()
                
    return spark

def clean_process(path):
    spark = start_session()
    title_ratings_schema = StructType([
        StructField('tconst', StringType(), False),
        StructField('averageRating', FloatType(), True),
        StructField('numVotes', IntegerType(), True)
    ])
    
    df = spark.read.csv(path, header=True, sep="\t", schema=title_ratings_schema)
    df.printSchema()
    df.show(2, vertical=True) 
    
    jdbc_url = "jdbc:postgresql://postgres:5432/challenge"
    connection_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }
    
    df.write.jdbc(
        url=jdbc_url,
        table="imdb.title_ratings",  # Specify the table name here
        mode="overwrite",  # Ensure the mode is set to overwrite
        properties=connection_properties
    )



    # df.write \
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql://postgres:5432/challenge") \
    #     .option("dbtable", "imdb.title_ratings") \
    #     .option("user", "admin") \
    #     .option("password", "admin") \
    #     .option("driver", "org.postgresql.Driver") \
    #     .option("cascadeTruncate", "true") \
    #     .save()
    


def main():
    
    start = timer()
    
    parser = argparse.ArgumentParser(description="Clean Process")
    
    parser.add_argument('--data_asset', type=str, dest="data_asset")
    parser.add_argument('--file_path', type=str, dest="file_path")
    
    args = parser.parse_args()
    data_asset = args.data_asset
    file_path = args.file_path
    print(f"Data asset: {data_asset}")
    print(f"File Path: {file_path}")
    
    clean_process(file_path)
    
    end = timer()
    print(end - start)
    


if __name__ == "__main__":
    main()