from confluent_kafka import Producer
import socket
from web3 import Web3
from websockets import connect
import asyncio
import json
import requests


web3 = Web3(Web3.HTTPProvider(infura_url))

async def get_event():
    async with connect(infura_url) as ws:
        await ws.send('{"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newPendingTransactions"]}')
        subscription_response = await ws.recv()
        print(subscription_response) #{"jsonrpc":"2.0","id":1,"result":"0xd67da23f62a01f58042bc73d3f1c8936"}

loop = asyncio.new_event_loop()
loop.run_until_complete(get_event())

print('done')
# conf = {'bootstrap.servers': 'localhost:9094'}

# producer = Producer(conf)

# print(producer)

# for i in range(100):
#   producer.produce('test', key=f"key{i}", value=f"value{i}")

# producer.flush()