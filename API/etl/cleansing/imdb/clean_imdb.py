from pyspark.sql import SparkSession

class CleanTaskIMDB:
    
    DB_SCHEMA = "imdb"

    JDBC_URL = "jdbc:postgresql://postgres:5432/challenge"
    CONN_PROPERTIES = {
            "user": "admin",
            "password": "admin",
            "driver": "org.postgresql.Driver"
        }


    def __init__(self, path):
        self.spark = SparkSession.builder \
            .config("spark.jars", "/opt/airflow/postgresql-42.7.3.jar") \
                .config("spark.executor.memory", "3g") \
                    .getOrCreate()
        self.path = path
                    
        
    def extract(self):
        return self.spark.read.csv(self.path, header=True, sep="\t", schema=self.SCHEMA)
        
    def load(self, df, table_name):
        
        df.write.jdbc(
            url=self.JDBC_URL,
            table=table_name, 
            mode="overwrite", 
            properties=self.CONN_PROPERTIES
        )

    def transform(self, df):
        return df
        
    def run(self):
        
        df = self.extract()
        
        df = self.transform(df)
        print(f"{self.DB_SCHEMA}.{self.DBTABLE_NAME}")
        self.load(df,
                  table_name = f"{self.DB_SCHEMA}.{self.DBTABLE_NAME}")
        
        self.spark.stop()