from pyspark.sql import SparkSession

class CleanTaskIMDB:
    
    DB_SCHEMA = "imdb"

    JDBC_URL = "jdbc:postgresql://postgres:5432/challenge"
    CONN_PROPERTIES = {
            "user": "admin",
            "password": "admin",
            "driver": "org.postgresql.Driver",
            "fetchsize": "1000",
            "batchsize": "500000",
            "truncate": True
        }

    def __init__(self, path):
        self.spark = SparkSession.builder \
            .config("spark.jars", "/opt/airflow/postgresql-42.7.3.jar") \
                .config("spark.executor.memory", "2g") \
                    .config("spark.executor.cores", "4") \
                        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=200")\
                            .getOrCreate()
        self.read_path = path
        
        
    def extract(self):
        
        return self.spark.read.csv(self.read_path, header=True, sep="\t", schema=self.SCHEMA)
        
    def load_db(self, df, table_name):
        
        # Save to the Database
        df.write.jdbc(
            url=self.JDBC_URL,
            table=table_name, 
            mode="overwrite", 
            properties=self.CONN_PROPERTIES
        )

    def transform(self, df):
        # Used in a normal setting but it takes to much time
        # df = df.distinct()
        
        return df
        
    def run(self):
        
        df = self.extract()
        
        df = self.transform(df)
            
        print(f"{self.DB_SCHEMA}.{self.DBTABLE_NAME}")
        
        self.load_db(df, table_name = f"{self.DB_SCHEMA}.{self.DBTABLE_NAME}")
          
        self.spark.stop()
        
        
        
        