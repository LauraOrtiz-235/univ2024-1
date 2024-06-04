import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.appName("lab_APISpark").getOrCreate()
sc = spark.sparkContext

movie_schema = StructType().add("movieId", "integer").add("title", "string").add("genres", "string")
rating_schema = StructType().add("userId", "integer").add("movieId", "integer").add("rating", "double").add("timestamp", "integer")

df_movies = spark.read.format("csv").option("header", "true").schema(movie_schema). \
            load("/home/laura/Desktop/univ/univ2024-1/BigData/lab/LabSpark2/ml-25m/movies.csv")
df_ratings = spark.read.format("csv").option("header", "true").schema(rating_schema). \
            load("/home/laura/Desktop/univ/univ2024-1/BigData/lab/LabSpark2/ml-25m/ratings.csv")

#df_movies.show()
#df_ratings.show()
df_movies.printSchema()
df_ratings.printSchema()

# Primer punto: Porcentaje de usuarios que le han dado a las películas mas de 3 estrellas?
average_ratings = df_ratings.groupBy("movieId").agg(avg("rating").alias("avg_rating"))

# Filtrar las películas con una calificación promedio superior a 3 estrellas
popular_movies = average_ratings.filter(average_ratings["avg_rating"] > 3)

# Calcular el total de usuarios que han calificado estas películas
total_users = df_ratings.select("userId").distinct().count()

# Calcular el total de usuarios que han calificado las películas populares
users_popular_movies = df_ratings.join(popular_movies, "movieId").select("userId").distinct().count()

# Calcular el porcentaje de usuarios
percentage_users = (users_popular_movies / total_users) * 100

print(f"El porcentaje de usuarios que han dado a las películas una calificación promedio superior a 3 estrellas es: {percentage_users}%")