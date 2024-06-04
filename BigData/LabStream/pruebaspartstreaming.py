from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("PruebaSparkStreaming").getOrCreate()

lines = (spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load())
words = lines.select(split(col("value"),"\\s").alias("word"))
counts = words.groupBy("word").count()

checkpointDir = "/home/laura/Desktop/univ/univ2024-1/BigData/lab/LabStream"
streamingQuery = (counts.writeStream.format("console").outputMode("update").trigger(processingTime="5 seconds").option("checkpointLocation", checkpointDir) .start())

streamingQuery.awaitTermination()