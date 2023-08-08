import pandas as pd
import numpy as np
from web3.providers import JSONBaseProvider
import time
from app.providers import Neo4jProvider, Web3Provider
from .ethereum_utils import send_batch
from .contract_service import ContractService
import os
import itertools
from typing import List
from .neo4j_queries import reset_data_query, load_block_from_csv_query, load_txs_from_csv_query, \
    load_addresses_from_csv_query, create_tx_account_relations_query, create_block_tx_relations_query
from app.helpers.file_helper import is_file_empty

from . import constants


class EthereumService:
    """Ethereum service using Pandas"""
    def __init__(self, blocks_to_export):
        self.infura_url = Web3Provider().infura_node_url
        self.web3 = Web3Provider().provider
        self.base_provider = JSONBaseProvider()
        self.neo4j_driver = Neo4jProvider().driver
        self.contract_service = ContractService()
        self.api_key = os.getenv('ETHERSCAN_API_KEY')
        self.BLOCK_TO_EXPORT = blocks_to_export

    def __del__(self):
        self.neo4j_driver.close()

    @staticmethod
    def _get_current_block_numbers():
        return (pd.read_csv(constants.BLOCKS_CSV_PATH, sep='\t')['number'].values
                if not is_file_empty(constants.BLOCKS_CSV_PATH)
                else pd.DataFrame()
                )

    @staticmethod
    def _get_current_txs():
        return (pd.read_csv(constants.TXS_CSV_PATH, sep='\t')
                if not is_file_empty(constants.TXS_CSV_PATH)
                else pd.DataFrame()
                )

    @staticmethod
    def _save_blocks_to_csv(blocks):
        if not is_file_empty(constants.BLOCKS_CSV_PATH):
            blocks.to_csv(constants.BLOCKS_CSV_PATH, sep='\t', index=True, mode='a', header=None)
        else:
            blocks.to_csv(constants.BLOCKS_CSV_PATH, sep='\t', index=True)

    @staticmethod
    def _save_txs_to_csv(txs):
        if not is_file_empty(constants.TXS_CSV_PATH):
            txs.to_csv(constants.TXS_CSV_PATH, sep='\t', index=False, mode='a', header=None)
        else:
            txs.to_csv(constants.TXS_CSV_PATH, sep='\t', index=False)

    @staticmethod
    def _save_addresses_to_csv(addresses):
        if not is_file_empty(constants.ADDRESSES_CSV_PATH):
            addresses.to_csv(constants.ADDRESSES_CSV_PATH, sep='\t', index=False, mode='a', header=None)
        else:
            addresses.to_csv(constants.ADDRESSES_CSV_PATH, sep='\t', index=False)

    @staticmethod
    def _save_abis_to_csv(abis):
        if not is_file_empty(constants.CONTRACTS_CSV_PATH):
            abis.to_csv(constants.CONTRACTS_CSV_PATH, sep='\t', index=False, mode='a', header=None)
        else:
            abis.to_csv(constants.CONTRACTS_CSV_PATH, sep='\t', index=False)

    def _send_batch_transactions(self, tx_hashes):
        results = send_batch(
            self.infura_url,
            self.base_provider,
            'eth_getTransactionByHash',
            tx_hashes
        )
        return [tx['result'] for tx in results]

    def _send_batch_blocks(self, block_numbers):
        results = send_batch(
            self.infura_url,
            self.base_provider,
            'eth_getBlockByNumber',
            block_numbers
        )
        return [block['result'] for block in results]

    def _get_n_latest_blocks(self, n) -> (pd.DataFrame, List[str]):
        current_blocks_numbers = self._get_current_block_numbers()

        latest_block_number = self.web3.eth.get_block('latest')['number']
        batch_blocks_arguments = [
            [hex(number), False]
            for number in range(latest_block_number - n, latest_block_number)
        ]

        blocks_data = self._send_batch_blocks(batch_blocks_arguments)

        tx_hashes = list(itertools.chain.from_iterable(
            block['transactions']
            for block in blocks_data if block['number'] not in current_blocks_numbers or current_blocks_numbers.empty)
        )

        return (pd.DataFrame(
                    data=blocks_data,
                    columns=['hash', 'size', 'number']
                )
                .fillna({'hash': 0})
                .assign(hash=lambda df_: df_['hash'].astype(str).apply(int, base=16))
                .loc[lambda df_: ~df_['number'].isin(current_blocks_numbers)]
                .set_index('number')
                ), tx_hashes

    def _get_txs_details(self, tx_hashes):
        return (pd.DataFrame(
                    data=self._send_batch_transactions(tx_hashes),
                    columns=['from', 'to', 'hash', 'blockNumber', 'value', 'blockHash', 'chainId', 'input'],
                )
                .fillna({'chainId': 0, 'blockNumber': 0})
                .assign(
                    chainId=lambda df_: df_['chainId'].astype(str).apply(int, base=16),
                    blockNumber=lambda df_: df_['blockNumber'].astype(str).apply(int, base=16)
                )
                .merge(pd.read_csv(constants.CHAINLIST_CSV_PATH, sep="\t", header=0), left_on='chainId',
                       right_on='chain_id')
                )

    def _get_addresses_from_txs(self, txs):
        current_txs = self._get_current_txs()

        def _get_account_type(df_):
            results = send_batch(
                self.infura_url,
                self.base_provider,
                'eth_getCode',
                [[self.web3.toChecksumAddress(address), 'latest'] for address in df_['address'].values]
            )

            df_ = df_.join(pd.DataFrame.from_records(data=results, columns=['result']))
            df_['type'] = np.where(df_['result'] == '0x', 'private', 'contract')

            return (df_['type']
                    .astype('category'))

        def _get_unique_addresses(df_):
            return (df_
                    .assign(address=list(txs['to']) + list(txs['from']))
                    .dropna()
                    .drop_duplicates(subset=['address'])
                    .pipe(
                        lambda df__: df__.loc[~df__['address'].isin([current_txs['from'], current_txs['to']])]
                        if not current_txs.empty else df__
                    )
                    ['address']
                    )

        return (pd.DataFrame()
                .assign(address=_get_unique_addresses)
                .assign(type=_get_account_type)
                )

    def _get_abis_from_addresses(self, addresses):
        return (pd.DataFrame()
                .assign(address=addresses['address'])
                .loc[lambda df_: addresses['type'] == 'contract']
                .assign(abi=lambda df_: self.contract_service.get_contracts_abi(df_['address']))
                .loc[lambda df_: df_['abi'] != 'Contract source code not verified']
                )

    def _get_data_to_export(self):
        def _decode_tx_functions(df_):
            decoded_tx = self.contract_service.decode_tx(df_['to'], df_['input'], df_['abi'])
            df_['function_name'] = decoded_tx[0]
            df_['function_params'] = decoded_tx[1]
            df_['target_schema'] = decoded_tx[2]
            return df_

        blocks, tx_hashes = self._get_n_latest_blocks(self.BLOCK_TO_EXPORT)
        txs = self._get_txs_details(tx_hashes)
        addresses = self._get_addresses_from_txs(txs)
        abis = self._get_abis_from_addresses(addresses)

        txs = (txs
               .merge(abis, left_on='to', right_on='address', how='inner')
               .assign(function_name='', function_params='', target_schema='')
               .apply(_decode_tx_functions, axis=1)
               .drop(columns=['address'])
               )

        return blocks, txs, addresses, abis

    def _import_csv_to_neo4j(self):
        with self.neo4j_driver.session() as session:
            session.execute_write(reset_data_query)
            session.execute_write(load_block_from_csv_query)
            session.execute_write(load_txs_from_csv_query)
            session.execute_write(load_addresses_from_csv_query)
            session.execute_write(create_tx_account_relations_query)
            session.execute_write(create_block_tx_relations_query)

    def save_txs_to_neo4j(self):
        st = time.time()

        blocks, txs, addresses, abis = self._get_data_to_export()

        self._save_blocks_to_csv(blocks)
        self._save_txs_to_csv(txs)
        self._save_addresses_to_csv(addresses)
        self._save_abis_to_csv(abis)

        et = time.time()
        print('Execution time:', et - st, 'seconds')

        self._import_csv_to_neo4j()

    def sandbox(self):
        print('test', self)
