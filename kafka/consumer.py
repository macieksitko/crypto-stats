from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
import pyspark.sql.types as T
import pyspark.sql.functions as F
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

AddressSchema = T.StructType([
    T.StructField("address", T.StringType(), True),
    T.StructField("type", T.StringType(), True),
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
         .option("checkpointLocation", "./kafka/checkpoints/1") 
         .outputMode("append")
         .trigger(processingTime="5 seconds")
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
  .option("checkpointLocation", "./kafka/checkpoints/2") 
  .outputMode("append")
  .trigger(processingTime="5 seconds")
  .start()
)

tx_block_relation_query = (txs.writeStream
  .format("org.neo4j.spark.DataSource")
  .option("query", """MATCH (b:Block)
    MATCH (t:Tx {blockNumber: toString(b.number)})
    MERGE (b)-[:HAS_COINBASE {value: t.value}]->(t)-[:INCLUDED_IN]->(b)""")
  .option("checkpointLocation", "./kafka/checkpoints/3") 
  .outputMode("append")
  .trigger(processingTime="5 seconds")
  .start())

addresses = (txs
              .select(F.explode(F.array("from", "to"))
                      .alias("address"))
              .filter(F.col('address').isNotNull())
              .distinct()
              .withColumn('type',
                          F.when(col('address') == '0x', 'contract')
                          .otherwise('private')))

addresses_query = (addresses
  .writeStream
  .format("org.neo4j.spark.DataSource")
  .option("labels", ":Address")
  .option("node.keys", "address")
  .option("checkpointLocation", "./kafka/checkpoints/4") 
  .outputMode("append")
  .trigger(processingTime="5 seconds")
  .start()
)

tx_address_relation_query = (addresses.writeStream
  .format("org.neo4j.spark.DataSource")
  .option("query",
    """MATCH (t:Tx), (a_from:Address {address: t.from}), (a_to:Address {address: t.to})
    MERGE (a_from)-[:SENT {value: t.value}]->(t)-[:RECEIVED {value: t.value}]->(a_to)""")
  .option("checkpointLocation", "./kafka/checkpoints/5") 
  .outputMode("append")
  .trigger(processingTime="5 seconds")
  .start())

print("Running consumer...")

spark.streams.awaitAnyTermination()

