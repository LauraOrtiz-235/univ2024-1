
import sys, string
import os
import socket
import time
import operator
import boto3
import json
# from pyspark.sql import SparkSession
# from pyspark.streaming import StreamingContext

from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import explode ,split ,window
from pyspark.sql.types import IntegerType, DateType, StringType, StructType
from pyspark.sql.functions import sum,avg,max



if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("NasaLogSparkStreaming")\
        .getOrCreate() \
    
    spark.sparkContext.setLogLevel("ERROR")
    # TODO: Define a windowDuration and slideDuration
    # Se definen dos variables windowDuration y slideDuration, 
    # que se utilizan más adelante en la consulta de Spark Streaming. 
    # Estas variables determinan la duración de la ventana y el deslizamiento
    # para el procesamiento de datos en tiempo real
    
    windowDuration = '50 seconds'
    slideDuration = '30 seconds'
        
    # Set up the `logsDF` readStream to take in data from a socket stream, AND include the timestamp
    # Transformación de  los datos leídos. Se divide cada línea de registro en palabras utilizando split()
    # y se crea una nueva columna "logs" con el resultado. Luego, se selecciona la columna de marca de tiempo 
    # (logsDF.timestamp) y se aplica la función explode() para convertir las palabras divididas en filas
    # separadas con la misma marca de tiempo.
    
    logsDF = spark.readStream.format("socket").option("host", 'localhost')\
             .option("port", 9999).option('includeTimestamp', 'true').load()


    # Splitting the lines and appending the current received timestamp on each log
    logsDF = logsDF.select(explode(split(logsDF.value, " ")).alias("logs"),logsDF.timestamp)
    
    
    # Add a watermark to the data, using the timestamp
    logsDF = logsDF.withWatermark("timestamp", "5 seconds")

    # splitting the log text into feature and giving them names
    logsDF = logsDF.withColumn('idx', split(logsDF['logs'], ',').getItem(0)) \
       .withColumn('hostname', split(logsDF['logs'], ',').getItem(1)) \
       .withColumn('time', split(logsDF['logs'], ',').getItem(2)) \
        .withColumn('method', split(logsDF['logs'], ',').getItem(3)) \
        .withColumn('resource', split(logsDF['logs'], ',').getItem(4)) \
        .withColumn('responsecode', split(logsDF['logs'], ',').getItem(5)) \
        .withColumn('bytes', split(logsDF['logs'], ',').getItem(6)) 


    #based on logs, count the number of attempts each host made to access the server.
    hostCountsDF = logsDF.groupBy(window(logsDF.timestamp, windowDuration, slideDuration),logsDF.hostname).count()
        
    # display all unbounded table completely while streaming the hostCountDF on console using output mode "complete"
    #query = hostCountsDF.writeStream.outputMode("complete")\
    #                   .option("numRows", "100000")\
    #                   .option("truncate", "false")\
    #                   .format("console")\
    #                   .start()

    ## Practice and observe by uncommenting the below code lines using different outputModel "append" , "update"
    
    ### display unbounded table withonly the changes are appended
    #query = hostCountsDF.writeStream.outputMode("append")\
    #                    .option("numRows", "100000")\
    #                    .option("truncate", "false")\
    #                    .format("console")\
    #                    .start()


    ### display unbounded with only the update 
    query = hostCountsDF.writeStream.outputMode("update")\
                        .option("numRows", "100000")\
                        .option("truncate", "false")\
                        .format("console")\
                        .start()

    
    # run the query 
    query.awaitTermination()
