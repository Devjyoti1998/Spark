from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Section3/Marvel+names.txt")

lines = spark.read.text("file:///SparkCourse/Section3/Marvel+graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

leastPopular= connections.sort(func.col("connections").asc()).first()
leastPopularName = names.filter(func.col("id")==leastPopular[0]).select("name").first()

# heroes with minimum connections
min=connections.agg(func.min("connections")).first[0]
withoneconnection=connections.filter(func.col("connections")==min)
withoneconnectionnames=withoneconnection.join(names,"id")

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")
print(leastPopularName[0] + " is the least popular superhero with " + str(leastPopular[1]) + " co-appearances.")
withoneconnectionnames.show()
