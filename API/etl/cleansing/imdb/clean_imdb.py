import os

from pyspark.sql import SparkSession

class CleanTaskIMDB:
    
    DB_SCHEMA = "imdb"

    JDBC_URL = "jdbc:postgresql://postgres:5432/challenge"
    CONN_PROPERTIES = {
            "user": "admin",
            "password": "admin",
            "driver": "org.postgresql.Driver",
            "fetchsize": "1000",
            "batchsize": "50000",
        }

    # DATALAKE_CLEAN = "/mnt/datalake/clean/imdb"

    def __init__(self, path, operation):
        self.spark = SparkSession.builder \
            .config("spark.jars", "/opt/airflow/postgresql-42.7.3.jar") \
                .config("spark.executor.memory", "4g") \
                    .getOrCreate()
        self.read_path = path
        self.write_path = path.replace("raw", "clean")
        self.operation = operation
                    
        
    def extract(self):
        
        if self.operation == "clean":
            return self.spark.read.csv(self.read_path, header=True, sep="\t", schema=self.SCHEMA)
        else:
            return self.spark.read.parquet(self.read_path)
        
    def load_db(self, df, table_name):
        
        # Save to the Database
        df.write.jdbc(
            url=self.JDBC_URL,
            table=table_name, 
            mode="overwrite", 
            properties=self.CONN_PROPERTIES
        )
    
    def load_parquet(self, df):
        
        # Save individual files to the Datalake
        df.write.mode("overwrite").parquet(self.write_path)

    def transform(self, df):
        # Used in a normal setting but it takes to much time
        # df = df.distinct()
        
        return df
        
    def run(self):
        
        df = self.extract()
        
        if self.operation == "clean":
            df = self.transform(df)
            
        print(f"{self.DB_SCHEMA}.{self.DBTABLE_NAME}")
        
        if self.operation == "load":
            self.load_db(df, table_name = f"{self.DB_SCHEMA}.{self.DBTABLE_NAME}")
        elif self.operation == "clean":
            self.load_parquet(df)   
        
        self.spark.stop()
        
        
        
        