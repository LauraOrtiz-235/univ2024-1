#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Pipeline Example.
"""

# $example on$
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
# $example off$
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import when

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PipelineExample").getOrCreate()
    
    # Definir el esquema personalizado
    custom_schema = StructType([
        StructField("Tweet_ID", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("wq", StringType(), True),
        StructField("text", StringType(), True)
    ])

    # Leer los datos de entrenamiento desde el archivo CSV con el esquema personalizado
    training = spark.read.csv("twitter_training.csv", header=True, schema=custom_schema)
    training.show(20)

    # Quitar columnas que no se usan
    training = training.drop("Tweet_ID")
    training = training.drop("entity")

    # Convertir la columna "sentiment" a tipo double y asignar valores a la columna "label"
    #training = training.withColumn("label", when(training["wq"] == "Positive", 1.0)
    #                                       .when(training["wq"] == "Negative", 0.0)
    #                                       .otherwise(2.0))  

    #training = training.filter(training["label"].isNotNull())
    training2 = training.drop("wq")

    training.printSchema()

    # Configurar el pipeline
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
    lr = LogisticRegression(maxIter=10, regParam=0.001)

    # Crear el pipeline
    pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

    # Ajustar el pipeline a los documentos de entrenamiento
    model = pipeline.fit(training2)

    spark.stop()
"""
    # Leer los datos de prueba desde el archivo CSV con el mismo esquema personalizado
    custom_schema2 = StructType([
        StructField("Tweet_ID", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("text", StringType(), True)
    ])
    test = spark.read.csv("twitter_validation.csv", header=True, schema=custom_schema2)

    # Quitar columnas que no se usan
    test = test.drop("Tweet_ID")
    test = test.drop("entity")
    test = test.drop("sentiment")

    # Realizar predicciones sobre los documentos de prueba y seleccionar las columnas de interés
    prediction = model.transform(test)
    selected = prediction.select("text", "probability", "prediction")
    for row in selected.collect():
        text, prob, prediction = row
        print(
            "(%s) --> prob=%s, prediction=%f" % (
                text, str(prob), prediction
            )
        )
"""
