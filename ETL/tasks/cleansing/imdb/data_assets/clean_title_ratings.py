from cleansing.imdb.clean_imdb import CleanTaskIMDB

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

class TitleRatingsClean(CleanTaskIMDB):
    
    DBTABLE_NAME = "title_ratings"
    SCHEMA = StructType([
        StructField('tconst', StringType(), False),
        StructField('averageRating', FloatType(), True),
        StructField('numVotes', IntegerType(), True)
        ])

    def transform(self, df):
        df = super().transform(df)

        return df