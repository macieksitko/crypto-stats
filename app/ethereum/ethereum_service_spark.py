import pandas as pd
import numpy as np
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from app.providers import Web3Provider
from web3.providers import JSONBaseProvider
import os

from .contract_service import ContractService
from .ethereum_utils import send_batch
from itertools import chain
import pyspark.sql.types as T
import pyspark.sql.functions as F
from .constants import BlockSchema, TxSchema
import json
import requests

def executeRestApi(verb, url, body, page):
  #
  headers = {
      'Content-Type': "application/json",
      'User-Agent': "apache spark 3.x"
  }
  res = None
  # Make API request, get response object back, create dataframe from above schema.
  try:
    if verb == "get":
      res = requests.get("{url}/{page}".format(url=url,page=page), data=body, headers=headers)
    else:
      res = requests.post("{url}/{page}".format(url=url,page=page), data=body, headers=headers)
  except Exception as e:
    return e
  if res != None and res.status_code == 200:
    return json.loads(res.text)
  return None


class EthereumSparkService:
    """Ethereum service using PySpark"""
    def __init__(self, blocks_to_export):
        self.infura_url = Web3Provider().infura_node_url
        self.web3 = Web3Provider().provider
        self.base_provider = JSONBaseProvider()
        self.contract_service = ContractService()
        self.api_key = os.getenv('ETHERSCAN_API_KEY')
        self.BLOCK_TO_EXPORT = blocks_to_export
        self.spark: SparkSession = (SparkSession.builder
            .master('local[*]')
            .appName("crypto-stats")
            .config("neo4j.url", "bolt://localhost:7687")
            .config("neo4j.authentication.type", "basic")
            .config("neo4j.authentication.basic.username", "neo4j")
            .config("neo4j.authentication.basic.password", "password")
            .getOrCreate())

    def _send_batch_blocks(self, block_numbers):
        results = send_batch(
            self.infura_url,
            self.base_provider,
            'eth_getBlockByNumber',
            block_numbers
        )
        return [block['result'] for block in results]

    def _get_blocks_in_range(self, from_block_number, to_block_number):
        batch_blocks_arguments = [
            [hex(number), True]
            for number in range(from_block_number, to_block_number)
        ]

        blocks_data = self._send_batch_blocks(batch_blocks_arguments)
        txs_data = list(chain.from_iterable(
            map(lambda block: block['transactions'], blocks_data)))

        print(txs_data[10])
        return blocks_data, txs_data

    def generate_graph(self):
        spark = self.spark

        latest_block_number = self.web3.eth.get_block('latest')['number']
        latest_blocks, latest_txs = self._get_blocks_in_range(latest_block_number-10, latest_block_number)

        blocks = (spark.createDataFrame(
            data=latest_blocks,
            schema=BlockSchema)
            .withColumn('tx_count', F.size(F.col('transactions')))
            .drop('transactions'))

        txs = (spark.createDataFrame(
            data=latest_txs,
            schema=TxSchema)
        )

        addresses = (txs
                     .select(F.explode(F.array("from", "to"))
                             .alias("address"))
                     .filter(F.col('address').isNotNull())
                     .distinct()
                     .withColumn('type',
                                 F.when(F.col('address') == '0x', 'contract')
                                 .otherwise('private')))

        (blocks.write.format("org.neo4j.spark.DataSource")
         .option("labels", ":Block")
         .option("node.keys", "number")
         .mode("Overwrite")
         .save())

        (txs.write.format("org.neo4j.spark.DataSource")
         .option("labels", ":Tx")
         .option("node.keys", "hash")
         .mode("Overwrite")
         .save())

        (addresses.write.format("org.neo4j.spark.DataSource")
         .option("labels", ":Account")
         .option("node.keys", "address")
         .mode("Overwrite")
         .save())

        (txs.write
         .format("org.neo4j.spark.DataSource")
         .option("query", """MATCH (b:Block)
            MATCH (t:Tx {blockNumber: toString(b.number)})
            MERGE (b)-[:HAS_COINBASE {value: t.value}]->(t)-[:INCLUDED_IN]->(b)""")
         .mode("Overwrite")
         .save())

        (addresses.write
         .format("org.neo4j.spark.DataSource")
         .option("query",
            """MATCH (t:Tx), (a_from:Address {address: t.from}), (a_to:Address {address: t.to})
            MERGE (a_from)-[:SENT {value: t.value}]->(t)-[:RECEIVED {value: t.value}]->(a_to)""")
         .mode("Overwrite")
         .save())
