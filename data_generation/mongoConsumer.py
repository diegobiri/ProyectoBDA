from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

aws_access_key_id='test'
aws_secret_access_key='test'

spark = SparkSession.builder \
    .appName("Leer y procesar con Spark") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://proyectobda-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", 'test') \
    .config("spark.hadoop.fs.s3a.secret.key", 'test') \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
    
df =spark  \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9093") \
  .option("subscribe", "clientes_stream") \
  .option("failOnDataLoss", "false") \
  .load()
  

schema = StructType() \
    .add("id_cliente", IntegerType()) \
    .add("nombre", StringType()) \
    .add("direccion", StringType()) \
    .add("preferencias_alimenticias", StringType())

# Convert value column to JSON and apply schema
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")



# Start the computation
query = df \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path","s3://new-sample-bucket/clientes_json") \
    .option("checkpointLocation","s3://new-sample-bucket/clientes") \
    .option("multiline","true")\
    .start()

# Print schema of DataFrame for debugging
df.printSchema()


# Wait for the termination of the query
query.awaitTermination()