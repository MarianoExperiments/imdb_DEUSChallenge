from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Python Spark SQL example") \
    .config("spark.jars", "/opt/spark_jars/postgresql-42.2.27.jre6.jar") \
    .getOrCreate()


df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/challenge") \
    .option("query", "SELECT * FROM public.dag") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()
