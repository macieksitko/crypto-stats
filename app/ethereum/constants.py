import os
from app.definitions import ROOT_DIR
import pyspark.sql.types as T

BLOCKS_CSV_PATH = os.path.join(ROOT_DIR, 'data_io', 'blocks.csv')
TXS_CSV_PATH = os.path.join(ROOT_DIR, 'data_io', 'txs.csv')
ADDRESSES_CSV_PATH = os.path.join(ROOT_DIR, 'data_io', 'addresses.csv')
CONTRACTS_CSV_PATH = os.path.join(ROOT_DIR, 'data_io', 'contracts.csv')
CHAINLIST_CSV_PATH = os.path.join(ROOT_DIR, 'data_io', 'chainlist.csv')

# Spark schemas
BlockSchema = T.StructType([
    T.StructField("number", T.StringType(), True),
    T.StructField("hash", T.StringType(), True),
    T.StructField("size", T.StringType(), True),
    T.StructField("transactions", T.ArrayType(T.StringType()), True),
    T.StructField('tx_count', T.IntegerType(), True)
])

TxSchema = T.StructType([
    T.StructField("hash", T.StringType(), True),
    T.StructField("from", T.StringType(), True),
    T.StructField("to", T.StringType(), True),
    T.StructField("value", T.StringType(), True),
    T.StructField("blockNumber", T.StringType(), True),
])

AddressSchema = T.StructType([
    T.StructField("address", T.StringType(), True),
    T.StructField("type", T.StringType(), True),
])