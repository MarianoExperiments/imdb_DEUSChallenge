from cleansing.imdb.clean_imdb import CleanTaskIMDB

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class NameBasicsClean(CleanTaskIMDB):
    
    DBTABLE_NAME = "name_basics"
    SCHEMA = StructType([
        StructField('nconst', StringType(), False),
        StructField('primaryName', StringType(), True),
        StructField('birthYear', IntegerType(), True),
        StructField('deathYear', IntegerType(), True),
        StructField('primaryProfession', StringType(), True),
        StructField('tconst', StringType(), True)
    ])

    def transform(self, df):
        
        df = df.withColumn("tconst", F.explode(F.split(F.col("tconst"), ",")))
        
        df = super().transform(df)

        return df