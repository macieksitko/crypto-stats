from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
import pyspark.sql.types as T
import findspark

BlockSchema = T.StructType([
    T.StructField("number", T.StringType(), False),
    T.StructField("hash", T.StringType(), True),
    T.StructField("size", T.StringType(), True),
])

TxSchema = T.StructType([
    T.StructField("hash", T.StringType(), False),
    T.StructField("from", T.StringType(), True),
    T.StructField("to", T.StringType(), True),
    T.StructField("value", T.StringType(), True),
    T.StructField("blockNumber", T.StringType(), True),
])

findspark.init()

spark: SparkSession = (SparkSession.builder
    .master('local[*]')
    .appName("crypto-stats")
    .config("neo4j.url", "bolt://localhost:7687")
    .config("neo4j.authentication.type", "basic")
    .config("neo4j.authentication.basic.username", "neo4j")
    .config("neo4j.authentication.basic.password", "password")
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0')  
    .config("kafkfa.offset.strategy","latest")
    .getOrCreate())

blocks = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9094")
  .option("failOnDataLoss", "false")
  .option("subscribe", "blocks")
  .load()
  .withColumn("value", from_json(col("value").cast("string"), BlockSchema))
  .select(col("value.*"))
)

txs = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9094")
  .option("failOnDataLoss", "false")
  .option("subscribe", "txs")
  .load()
  .selectExpr("CAST(value AS STRING)")
  .withColumn("data", from_json(col("value"), TxSchema))
  .select("data.*")
)

blocks.printSchema()
txs.printSchema()

blocks_query = (blocks
         .writeStream
         .format("org.neo4j.spark.DataSource")
         .option("labels", ":Block")
         .option("node.keys", "number")
         .option("checkpointLocation", "./kafka/checkpoint") 
         .outputMode("append")
         .trigger(continuous="10 seconds")
         .start()
)

# For debugging
# query = txs.writeStream.format("console").start()
# import time
# time.sleep(10) # sleep 10 seconds
# query.stop()

tx_query = (txs
         .writeStream
         .format("org.neo4j.spark.DataSource")
         .option("labels", ":Tx")
         .option("node.keys", "hash")
         .option("checkpointLocation", "./kafka/checkpoint") 
         .outputMode("append")
         .trigger(continuous="5 seconds")
         .start()
)


print("Running consumer...")
tx_query.awaitTermination()

