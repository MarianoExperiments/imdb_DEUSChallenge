from cleansing.imdb.clean_imdb import CleanTaskIMDB

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class TitlePrincipalsClean(CleanTaskIMDB):
    
    DBTABLE_NAME = "title_principals"
    SCHEMA = StructType([
        StructField('tconst', StringType(), False),
        StructField('ordering', IntegerType(), True),
        StructField('nconst', StringType(), True),
        StructField('category', StringType(), True),
        StructField('job', StringType(), True),
        StructField('characters', StringType(), True)
    ])

    def transform(self, df):
        df = super().transform(df)

        return df