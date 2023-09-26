import os
from web3 import Web3


class Web3Provider:
    def __init__(self):
        self.infura_node_url = os.getenv('INFURA_NODE_URL')
        self.provider = Web3(Web3.HTTPProvider(self.infura_node_url))

