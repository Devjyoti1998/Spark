from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

schema = StructType([ \
                     StructField("cust_id", StringType(), True), \
                     StructField("item_id", IntegerType(), True), \
                     StructField("amt_spent", FloatType(), True)])
customer = spark.read.schema(schema).csv("file:///SparkCourse/Section2/customer-orders.csv")


customer.groupBy("cust_id").agg(func.round(func.sum("amt_spent"),2).alias("total")).orderBy("total").show(customer.count())
spark.stop()
