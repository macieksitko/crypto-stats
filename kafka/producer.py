from confluent_kafka import Producer, SerializingProducer
from web3 import Web3
from websockets import connect
from dotenv import load_dotenv
import asyncio
import json
import os


load_dotenv()
    
ws_infura = os.getenv('INFURA_WSS_URL')
http_infura = os.getenv('INFURA_NODE_URL')

web3 = Web3(Web3.HTTPProvider(http_infura))

def json_serializer(msg, s_obj):
    return json.dumps(msg).encode('utf-8')

conf = {
    'bootstrap.servers': 'localhost:9094',
    'value.serializer': json_serializer
}

producer = SerializingProducer(conf)

async def get_event():
    async with connect(ws_infura) as ws:
        await ws.send('{"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}')
        subscription_response = await ws.recv()
        print(subscription_response)

        while True:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=15)
                response = json.loads(message)
                result = response['params']['result']

                block = web3.eth.get_block(result['number'], True)

                block_data = {
                    "number": str(block["number"]),
                    "hash": str(block["hash"]),
                    "size": str(block["size"]),
                }

                producer.produce('blocks', value=block_data)
                print("Blocks messages: ", len(producer))

                for tx in block['transactions']:
                  tx_data = {
                      "hash": str(tx["hash"].hex()),
                      "from": str(tx["from"]),
                      "to": str(tx["to"]),
                      "value": str(tx["value"]),
                      "blockNumber": str(tx["blockNumber"])
                  }

                  if tx_data["hash"] == None:
                      print(tx_data)

                  producer.produce('txs', value=tx_data)
                  
                print("Tx messages: ", len(producer))
                producer.flush()
            except Exception as e:
                print(e)

if __name__ == "__main__":
    print("Running producer...")
    loop = asyncio.get_event_loop()

    while True:
        loop.run_until_complete(get_event())
