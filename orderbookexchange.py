import base64
import requests
import json
from google.cloud import pubsub_v1
import pandas as pd
from pandas.core.reshape.concat import concat
from pandas.core.frame import DataFrame
from google.cloud import bigquery
from google.cloud import storage


bq_project_name = "coredata-trial"
bq_dataset_name = "orderbookdataset"
bq_table_name   = "orderbookcrypto"

bq_table_full_path = f"""{bq_project_name}.{bq_dataset_name}.{bq_table_name}"""
bq_client = bigquery.Client(bq_project_name)

def write_to_bigquery(message: dict):

    errors =  bq_client.insert_rows_json(
        bq_table_full_path,
        message,  # Must be a list of objects, even if only 1 row.
    )
    for error in errors:
        print(f"encountered error: {error}")

def store_data_in_bucket(df: bytes):
    # Instantiates a client
    project_id = "coredata-trial"
    client = storage.Client(project_id)
    # Creates a new bucket and uploads an object
    fname = "orderbook" + pd.to_datetime('now').strftime("%Y-%m-%d-%H-%M")+".json"
    bucket = client.bucket("coredatastore001")
    blob = bucket.blob(fname)
    blob.upload_from_string(
          data=df,
          content_type='application/json'
        )    
    print(f"Wrote json with pandas with name {blob.name} to the bucket {bucket.name}.")

def covert_ob_to_dataframe_binance(obdata: dict, exchange, symbol) -> DataFrame:
    obframes = {side: pd.DataFrame(data=obdata[side], columns=['price', 'quantity'], dtype=float) for side in ["bids", "asks"]}
    obframes_list = [obframes[side].assign(side=side) for side in obframes] 
    _obdata = pd.concat(obframes_list, axis='index', ignore_index=True, sort=True)
    #range_series = _obdata["price"].between(MIN_VALUE, MAX_VALUE, inclusive = True)
    _obdata ['cost']  = _obdata ['price'] * _obdata ['quantity']
    _obdata['datatimeload'] = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M")
    _obdata['exchange'] = exchange
    _obdata['symbol'] = symbol
    return _obdata


def covert_ob_to_dataframe_coinbase(obdata: dict, exchange, symbol) -> DataFrame:
    obframes = {side: pd.DataFrame(data=obdata[side], columns=['price', 'quantity', 'size'], dtype=float) for side in ["bids", "asks"]}
    obframes_list = [obframes[side].assign(side=side) for side in obframes] 
    _obdata = pd.concat(obframes_list, axis="index", ignore_index=True, sort=True)
    _obdata = _obdata.drop('size', 1)
    #range_series = _obdata["price"].between(MIN_VALUE, MAX_VALUE, inclusive = True)
    #_obdata  =_obdata[range_series]
    _obdata ['cost']  = _obdata ['price'] * _obdata ['quantity']
    _obdata['datatimeload'] = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M")
    _obdata['exchange'] = exchange
    _obdata['symbol'] = symbol
    return _obdata


def hello_pubsub(event, context): 
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    project_id = "coredata-trial"
    topic_id = "Orderbookfromexchanges"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    
    # get some orderbook data

    binance_btc_api = "https://www.binance.com/api/v3/depth?symbol=BTCBUSD&limit=1000"
    binance_eth_api = "https://www.binance.com/api/v3/depth?symbol=ETHBUSD&limit=1000"
    coinbase_btc_api="https://api.pro.coinbase.com/products/BTC-USD/book?level=2"
    coinbase_eth_api="https://api.pro.coinbase.com/products/ETH-USD/book?level=2"

    obreq_binance_btc =  requests.get(binance_btc_api).json();
    obreq_binance_eth =  requests.get(binance_eth_api).json();
    obreq_coinbase_btc =  requests.get(coinbase_btc_api).json();
    obreq_coinbase_eth =  requests.get(coinbase_eth_api).json();

    pd_bin_btc = covert_ob_to_dataframe_binance(obreq_binance_btc,"Binance", "BTC")
    pd_bin_eth = covert_ob_to_dataframe_binance(obreq_binance_eth,"Binance", "ETH")
    pd_cb_btc = covert_ob_to_dataframe_coinbase(obreq_coinbase_btc,"Coinbase", "BTC")
    pd_cb_eth = covert_ob_to_dataframe_coinbase(obreq_coinbase_eth,"Coinbase", "ETH")
    
    obframes_pd = [pd_bin_btc, pd_bin_eth, pd_cb_btc, pd_cb_eth ] 
    _obdata_list = pd.concat(obframes_pd)
    _obdata_list = _obdata_list[_obdata_list.cost >=40000]
    ob_dictionary = [ dict([(colname, row[i])  for i,colname in enumerate(_obdata_list.columns)  ])  for row in _obdata_list.values ]  
    ob_message = json.dumps(ob_dictionary).encode("utf-8")
    store_data_in_bucket(ob_message)
    write_to_bigquery(json.loads(ob_message))
    
    