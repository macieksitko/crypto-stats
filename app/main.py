import os
import sys
from ethereum.ethereum_service import EthereumService
from ethereum.ethereum_service_spark import EthereumSparkService
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from definitions import ROOT_DIR
import findspark
findspark.init()

print("Using spark version: ", findspark.find())
def test_pyspark():
    eth = EthereumSparkService(100)
    eth.generate_graph()

def main():
    load_dotenv()
    # eth = EthereumService(1)
    # eth.save_txs_to_neo4j()
    # eth.sandbox()
    test_pyspark()

if __name__ == '__main__':
    # TODO: check if that make sense
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    main()
    print('Done;)')
