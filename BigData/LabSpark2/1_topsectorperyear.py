import pyspark 
import re
"""
# Creación de un contexto de Spark
WORD_REGEX = re.compile(r"\b\w+\b")
sc = pyspark.SparkContext()

# Importamos los datasets
nasdaq = sc.textFile("/home/laura/Desktop/univ/univ2024-1/BigData/proyecto/proyecto2/input/NASDAQsample.csv")
company = sc.textFile("/home/laura/Desktop/univ/univ2024-1/BigData/proyecto/proyecto2/input/companylist.tsv")

sector = company.flatMap(lambda l: l.split())
result = sector.map(lambda w: (w.lower(), 1).reduceByKey(lambda a, b: a + b))
result.saveAsTextFile("/home/laura/Desktop/univ/univ2024-1/BigData/proyecto/proyecto2/output/sector")
"""           
sc = pyspark.SparkContext("local", "SectorOperationsAnalysis")

# Cargar los datos de NASDAQ y companylist
nasdaq_rdd = sc.textFile("ruta/a/nasdaq.csv")
companylist_rdd = sc.textFile("ruta/a/companylist.csv")

# Función para mapear las líneas de NASDAQ a (año, sector) con el volumen de operaciones
def map_nasdaq(line):
    fields = line.split(',')
    return (fields[2][:4], (fields[0], int(fields[7])))

# Función para mapear las líneas de companylist a (año, sector)
def map_companylist(line):
    fields = line.split(',')
    return (fields[2], fields[3])

# Mapear NASDAQ
nasdaq_mapped = nasdaq_rdd.map(map_nasdaq)

# Mapear companylist
companylist_mapped = companylist_rdd.map(map_companylist)

# Unir NASDAQ y companylist por año
joined_rdd = nasdaq_mapped.join(companylist_mapped)

# Calcular el total de operaciones por sector y año
sector_year_operations = joined_rdd.map(lambda x: ((x[1][1], x[0]), x[1][0][1])) \
                                    .reduceByKey(lambda a, b: a + b)

# Encontrar el sector con el mayor número de operaciones por año
max_operations_per_year = sector_year_operations.map(lambda x: ((x[0][0], x[0][1]), x[1])) \
                                                .reduceByKey(max)

# Imprimir el resultado
for ((sector, year), total_operations) in max_operations_per_year.collect():
    print(f"{sector},{year},{total_operations}")
