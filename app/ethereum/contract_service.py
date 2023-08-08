import os
import numpy as np
import requests
import sys
from functools import lru_cache
from eth_utils import to_hex
import json
from app.providers import Web3Provider
from tqdm import tqdm
import pandas as pd

class ContractService:
    def __init__(self):
        self.api_key = os.getenv('ETHERSCAN_API_KEY')
        self.web3 = Web3Provider().provider

    def get_contracts_abi(self, addresses):
        abis = []
        for address in tqdm(addresses):
            query = {
                'module': 'contract',
                'action': 'getabi',
                'address': address,
                'apikey': self.api_key,
            }
            try:
                r = requests.get('http://api.etherscan.io/api', params=query)
                abis.append(json.loads(r.content)['result'])
            except requests.exceptions.RequestException as e:  # This is the correct syntax
                raise BaseException(e)
        return abis

    def decode_tuple(self, t, target_field):
        output = dict()
        for i in range(len(t)):
            if isinstance(t[i], (bytes, bytearray)):
                output[target_field[i]['name']] = to_hex(t[i])
            elif isinstance(t[i], tuple):
                output[target_field[i]['name']] = self.decode_tuple(t[i], target_field[i]['components'])
            else:
                output[target_field[i]['name']] = t[i]
        return output

    def decode_list_tuple(self, l, target_field):
        output = l
        for i in range(len(l)):
            output[i] = self.decode_tuple(l[i], target_field)
        return output

    def decode_list(l):
        output = l
        for i in range(len(l)):
            if isinstance(l[i], (bytes, bytearray)):
                output[i] = to_hex(l[i])
            else:
                output[i] = l[i]
        return output

    def convert_to_hex(self, arg, target_schema):
        """
        utility function to convert byte codes into human readable and json serializable data structures
        """
        output = dict()
        for k in arg:
            if isinstance(arg[k], (bytes, bytearray)):
                output[k] = to_hex(arg[k])
            elif isinstance(arg[k], list) and len(arg[k]) > 0:
                target = [a for a in target_schema if 'name' in a and a['name'] == k][0]
                if target['type'] == 'tuple[]':
                    target_field = target['components']
                    output[k] = self.decode_list_tuple(arg[k], target_field)
                else:
                    output[k] = self.decode_list(arg[k])
            elif isinstance(arg[k], tuple):
                target_field = [a['components'] for a in target_schema if 'name' in a and a['name'] == k][0]
                output[k] = self.decode_tuple(arg[k], target_field)
            else:
                output[k] = arg[k]
        return output

    @lru_cache(maxsize=None)
    def _get_contract(self, address, abi):
        """
        This helps speed up execution of decoding across a large dataset by caching the contract object
        It assumes that we are decoding a small set, on the order of thousands, of target smart contracts
        """
        if isinstance(abi, str):
            abi = json.loads(abi)

        contract = self.web3.eth.contract(
            address=self.web3.toChecksumAddress(address),
            abi=abi
        )
        return contract, abi

    def decode_tx(self, address, input_data, abi):
        if abi is not None:
            try:
                contract, abi = self._get_contract(address, abi)
                func_obj, func_params = contract.decode_function_input(input_data)
                target_schema = [a['inputs'] for a in abi if 'name' in a and a['name'] == func_obj.fn_name][0]
                decoded_func_params = self.convert_to_hex(func_params, target_schema)
                return pd.Series([func_obj.fn_name, json.dumps(decoded_func_params), json.dumps(target_schema)])
            except:
                #e = sys.exc_info()[0]
                return 'empty', 'empty', 'empty'
                #return 'decode error', repr(e), None
        else:
            return 'no matching abi', None, None
