from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

schema = StructType([ \
                     StructField("user_id", IntegerType(), True), \
                     StructField("movie_id", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", IntegerType(), True)])

movie = spark.read.option("sep","\t").schema(schema).csv("file:///SparkCourse/Section3/ml-100k/u.data")

movie.groupBy("movie_id").count().orderBy(func.desc("count")).show()
spark.stop()
#agg(func.desc("rating").alias("rating_desc")).
