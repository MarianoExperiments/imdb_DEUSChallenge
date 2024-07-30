from cleansing.imdb.clean_imdb import CleanTaskIMDB

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

class TitleBasicsClean(CleanTaskIMDB):
    
    DBTABLE_NAME = "title_basics"
    SCHEMA = StructType([
        StructField('tconst', StringType(), False),
        StructField('titleType', StringType(), True),
        StructField('primaryTitle', StringType(), True),
        StructField('originalTitle', StringType(), True),
        StructField('isAdult', BooleanType(), True),
        StructField('startYear', IntegerType(), True),
        StructField('endYear', IntegerType(), True),
        StructField('runtimeMinutes', IntegerType(), True),
        StructField('genres', StringType(), True)
    ])

    def transform(self, df):
        
        df = super().transform(df)

        return df