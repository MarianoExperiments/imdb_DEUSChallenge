import os
import json
import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class ProfessionalInfoRich:
    
    JDBC_URL = "jdbc:postgresql://postgres:5432/challenge"
    CONN_PROPERTIES = {
            "user": "admin",
            "password": "admin",
            "driver": "org.postgresql.Driver",
            "fetchsize": "1000",
            "batchsize": "50000",
        }


    def __init__(self, operation, paths=dict()):
        self.paths = paths
        self.operation = operation
        self.tmp_folder_path = "/mnt/datalake/clean/imdb/tmp"
        self.fix_final_path = "/mnt/datalake/clean/imdb/professional_info"
        self.table_name = "imdb.professional_info"
        
    def spark_session(self, app_mame):
        self.spark = SparkSession.builder \
                        .appName(app_mame)\
                            .config("spark.jars", "/opt/airflow/postgresql-42.7.3.jar") \
                                .config("spark.executor.memory", "6g") \
                                    .getOrCreate()

    def read_parquet(self, path):
        return self.spark.read.parquet(path)

    def write_df(self, df, path):
        df.write.mode("overwrite").parquet(path)
    
    def load_db(self, df):
        df.write.jdbc(
            url=self.JDBC_URL,
            table=self.table_name, 
            mode="overwrite", 
            properties=self.CONN_PROPERTIES
        )
    
    def join_1(self):
        self.spark_session("join_1")
        
        df_name_basics = self.read_parquet(self.paths["name.basics"])\
                                .filter((F.col("primaryProfession").contains("actor")) | (F.col("primaryProfession").contains("actress")))\
                                    .select(F.col("primaryName").alias("name"), "nconst")
                        
        df_title_principals = self.read_parquet(self.paths["title.principals"])\
                                    .filter(F.col("category").isin("actor", "actress"))\
                                        .select("nconst", "tconst", "category")
   
                                            
        df = df_name_basics.join(df_title_principals, how="inner", on="nconst").drop("nconst")
        self.write_df(df, os.path.join(self.tmp_folder_path, "tmp_1"))
        self.spark.stop() 
         
    def join_2(self):
        self.spark_session("join_2")
        
        df_tmp = self.read_parquet(os.path.join(self.tmp_folder_path, "tmp_1"))
        
        df_title_basics = self.read_parquet(self.paths["title.basics"])\
                                .filter(F.col("runtimeMinutes").isNotNull())\
                                    .select("tconst", "runtimeMinutes")
                                            
        df = df_tmp.join(df_title_basics, how="inner", on="tconst")
        self.write_df(df, os.path.join(self.tmp_folder_path, "tmp_2"))
        
        self.spark.stop()  
        
    def final_join(self):
        self.spark_session("join_3")
        
        df_tmp = self.read_parquet(os.path.join(self.tmp_folder_path, "tmp_2"))
        
        df_title_ratings = self.read_parquet(self.paths["title.ratings"])\
                                .select("tconst", "averageRating")
                                            
        df = df_tmp.join(df_title_ratings, how="inner", on="tconst")
        self.write_df(df, os.path.join(self.tmp_folder_path, "tmp_3"))
        
        self.spark.stop()  
        
    def distinct(self):
        self.spark_session("distinct")
        
        df = self.read_parquet(os.path.join(self.tmp_folder_path, "tmp_3")).distinct()
        
        self.write_df(df, self.fix_final_path)
        
        self.spark.stop()  
    
    def run(self):
        
        if self.operation == "clean":
            self.join_1()
            # self.join_2()
            # self.final_join()
            # self.distinct()   
        elif self.operation == "load":
            df = self.read_parquet(self.fix_final_path)
            self.load_db(df)
        
        

    
def main():

    parser = argparse.ArgumentParser(description="Get Paths for the dataassets")

    parser.add_argument('--paths', type=str, required=True, dest="paths")
    parser.add_argument('--operation', type=str, required=True, dest="operation")

    args = parser.parse_args()

    try:
        paths_dict = json.loads(args.paths)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON string: {e}")
        return

    operation = args.operation
    
    print("Paths received:", paths_dict)
    print("Operation received:", operation)
    
    obj = ProfessionalInfoRich(operation, paths_dict)
    obj.run()
    
    
if __name__ == "__main__":
    main()