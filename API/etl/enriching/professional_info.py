from pyspark.sql import SparkSession
import pyspark.sql.functions as F


JDBC_URL = "jdbc:postgresql://postgres:5432/challenge"
CONN_PROPERTIES = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver",
        "fetchsize": "1000",
        "batchsize": "50000",
    }


def spark_session(name):
    return SparkSession.builder \
            .appName(name) \
            .config("spark.jars", "/opt/airflow/postgresql-42.7.3.jar") \
            .config("spark.executor.memory", "4g") \
                .getOrCreate()

def read_table(spark, table_name):
    return spark.read.parquet(f"/mnt/datalake/clean/imdb/{table_name}")

def write_df(name, df):
    return df.write.mode("overwrite").parquet(f"/mnt/datalake/clean/imdb/professional_info/{name}")

# def first_join():
#     spark = spark_session("1")
#     df_name_basics = read_table(spark, "name_basics")\
#                             .filter(F.col("primaryProfession").isin("actor", "actress"))\
#                                 .select(F.col("primaryName").alias("name"), "nconst")\
#                                     .distinct()
                        
#     df_title_principals = read_table(spark, "title_principals")\
#                                 .filter(F.col("category").isin("actor", "actress"))\
#                                     .select("nconst", "tconst", "category")\
#                                         .distinct()
                         
#     df_final = df_name_basics.join(df_title_principals, how="inner", on="nconst").drop("nconst")
#     write_df("first_join", df_final)
    
#     spark.stop()
    
# def second_join():
#     spark = spark_session("2")
#     df_title_basics = read_table(spark, "title_basics")\
#                             .filter(F.col("runtimeMinutes").isNotNull())\
#                                 .select("tconst", "runtimeMinutes")\
#                                     .distinct()
                                
#     df_1_join = read_table(spark, "professional_info/first_join")
    
#     df_final = df_1_join.join(df_title_basics, how="inner", on="tconst")
#     write_df("second_join", df_final)
    
#     spark.stop()
    
# def final_join():
#     spark = spark_session("3")
    
#     df_title_ratings = read_table(spark, "title_ratings")\
#                             .select("tconst", "averageRating")
#     df_2_join = read_table(spark, "professional_info/second_join")
    
#     df_final = df_2_join.join(df_title_ratings, how="inner", on="tconst")
#     write_df("final", df_final)
    
#     spark.stop()

    
def main():
    
    # first_join()
    # second_join()
    # final_join()
    spark = spark_session("4")
    
    df_name_basics = read_table(spark, "name_basics")\
                            .filter((F.col("primaryProfession").contains("actor")) | (F.col("primaryProfession").contains("actress")))\
                                .select(F.col("primaryName").alias("name"), "nconst")\
                                    .distinct()
                        
    df_title_principals = read_table(spark, "title_principals")\
                                .filter(F.col("category").isin("actor", "actress"))\
                                    .select("nconst", "tconst", "category")\
                                        .distinct()
                                    
    df_title_basics = read_table(spark, "title_basics")\
                            .filter(F.col("runtimeMinutes").isNotNull())\
                                .select("tconst", "runtimeMinutes")\
                                    .distinct()
                                
    df_title_ratings = read_table(spark, "title_ratings")\
                            .select("tconst", "averageRating")\
                                .distinct()
    
    df = df_name_basics.join(df_title_principals, how="inner", on="nconst").drop("nconst")\
                        .join(df_title_basics, how="inner", on="tconst")\
                        .join(df_title_ratings, how="inner", on="tconst")
                            
    
    # df = read_table(spark, "professional_info/final").distinct()
    
    df.write.jdbc(
            url=JDBC_URL,
            table="imdb.professional_info", 
            mode="overwrite", 
            properties=CONN_PROPERTIES
        )
    spark.stop()
    
if __name__ == "__main__":
    main()