from pyspark.sql import SparkSession

# Configuración de las credenciales de AWS
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

ruta_restaurantes = "/opt/spark-data/json/restaurantes.json"
ruta_habitaciones = "/opt/spark-data/csv/habitaciones.csv"

try:
    # Crear una sesión de Spark con las configuraciones necesarias para acceder a S3 a través de Localstack
    spark = SparkSession.builder \
        .appName("SPARK S3") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://ProyectoBDA-localstack-1:4566") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Leer un archivo CSV desde el sistema de archivos local
    df3 = spark.read.option("delimiter", ",").option("header", True).csv(ruta_habitaciones)
    
    # Mostrar el esquema de los DataFrames
    df3.printSchema()
    
    
    # Escribir el DataFrame en formato CSV en un bucket S3
    df3.write \
        .option('fs.s3a.committer.name', 'partitioned') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
        .mode('overwrite') \
        .csv(path='s3a://new-sample-bucket/habitacionesData', sep=',')
    
    # Leer un archivo JSON desde el sistema de archivos local
    dfJson = spark.read.option("multiline", "true").json(ruta_restaurantes)
    
    dfJson.printSchema()
    
    # Escribir el DataFrame en formato JSON en un bucket S3
    dfJson.write \
        .option('fs.s3a.committer.name', 'partitioned') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
        .mode('overwrite') \
        .json(path='s3a://new-sample-bucket/restaurantesData')
    
    # Detener la sesión de Spark
    spark.stop()
    
except Exception as e:
    print(e)
