# Consider creating nodes like 'Crypto day' with specific info about tokens price each day

def load_block_from_csv_query(tx):
    result = tx.run("""LOAD CSV WITH HEADERS from 'file:///blocks.csv' as row 
        FIELDTERMINATOR '\t'
        MERGE (b:Block {id:row.hash, size:row.size, number: row.number})
        RETURN(b);""")

    summary = result.consume()
    return summary


def load_txs_from_csv_query(tx):
    result = tx.run("""LOAD CSV WITH HEADERS from 'file:///txs.csv' as row
        FIELDTERMINATOR '\t'
        WITH row WHERE (row.to is not null) and (row.blockNumber is not null)
        MERGE (t:Transaction {
            id:row.hash, from:row.from, to:row.to, blockNumber: row.blockNumber, 
            value: row.value, chainId: row.chain_id, chainSymbol: row.chain_symbol,
            chainName: row.chain_name, function_name: row.function_name,
            function_params: row.function_params, target_schema: row.target_schema
        })
        RETURN(t);""")

    summary = result.consume()
    return summary


def load_addresses_from_csv_query(tx):
    result = tx.run("""LOAD CSV WITH HEADERS from 'file:///addresses.csv' as row
        FIELDTERMINATOR '\t'
        MERGE (a: Address {address:row.address, type:row.type})
        RETURN(a);""")

    summary = result.consume()
    return summary


def create_tx_account_relations_query(tx):
    result = tx.run("""MATCH (t:Transaction), (a_from:Address {address: t.from}), (a_to:Address {address: t.to})
        MERGE (a_from)-[:SENT {value: t.value}]->(t)-[:RECEIVED {value: t.value}]->(a_to)""")

    summary = result.consume()
    return summary


def create_block_tx_relations_query(tx):
    result = tx.run("""MATCH (b:Block)
        MATCH (t:Transaction {blockNumber: toString(b.number)})
        MERGE (b)-[:HAS_COINBASE {value: t.value}]->(t)-[:INCLUDED_IN]->(b)""")

    summary = result.consume()
    return summary


def reset_data_query(tx):
    result = tx.run("""MATCH(n)
        DETACH DELETE n;""")
    summary = result.consume()
    return summary
