import requests
from web3.types import RPCResponse
from typing import List
from varname import nameof
import pandas as pd


def send_batch(node_url, base_provider, fun_name, params_set):
    request_data = b'[' + b','.join(
        [base_provider.encode_rpc_request(fun_name, params) if(type(params) == list)
         else base_provider.encode_rpc_request(fun_name, [params]) for params in params_set]
    ) + b']'

    r = requests.post(node_url, data=request_data, headers={'Content-Type': 'application/json'})

    responses: List[RPCResponse] = base_provider.decode_rpc_response(r.content)

    return responses

# def get_token_symbol(abiOfToken, addressOfToken):
    # tokenContract = web3.eth.Contract(abiOfToken, addressOfToken);
    #
    # const[decimals, name, symbol] = await Promise.all([
    # tokenContract.methods.symbol().call()
    # tokenContract.methods.decimals().call()
    # tokenContract.methods.name().call()
    # return {decimals, name, symbol};
