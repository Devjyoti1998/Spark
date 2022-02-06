from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/Section2/fakefriends-header.csv")



people.groupBy("age").agg(func.round(func.avg("friends"),2).alias("avg_friends")).orderBy("age").show()
spark.stop()
