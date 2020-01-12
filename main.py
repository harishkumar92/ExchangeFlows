import requests, sys
from datetime import datetime
import pandas as pd

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import load_dailyohlcv as load_daily


def get_exchanges():
    return ['binance', 'bittrex', 'bitstamp', 'bitmex', 'bitfinex', 'huobi', 'kraken', 'poloniex']

def get_stablecoins():
    return {'usdt_omni':['kraken', 'bitfinex', 'poloniex', 'bittrex', 'okex'], \
             'usdt_erc20': ['binance', 'bitfinex', 'bittrex', 'kucoin', 'poloniex'], \
             'usdc':['binance', 'bitfinex'], 'pax':['binance', 'bitfinex', 'bittrex']}


def get_data(token, exchange, direction, TA_API_KEY):
    req_url = """https://api.tokenanalyst.io/analytics/private/v1/exchange_flow_window_historical/last?format=json&token={0}&exchange={1}&direction={2}&window=1d&limit=2000&key={3}"""

    req_url = req_url.format(token, exchange, direction, TA_API_KEY)
    print (req_url)
    data = pd.DataFrame(requests.get(req_url).json())
    data['number_of_txns']
    data['avg_txn_value']
    data['avg_txn_value_usd']
 
    data['exchange'] = exchange
    data.index = data.date
    return data

def get_stablecoin_flows(TA_API_KEY):
    all_data=[]
    sc_exch = get_stablecoins()
    for sc in list(sc_exch.keys()):
        for exchange in sc_exch[sc]:
            exch_inflows = get_data(sc, exchange, 'inflow', TA_API_KEY)
            exch_outflows = get_data(sc, exchange, 'outflow', TA_API_KEY)
            data = pd.concat([exch_inflows, exch_outflows], axis=1, join='inner')
            all_data.append(data)
    
    all_data  = pd.concat(all_data)
    all_data = all_data.loc[:,~all_data.columns.duplicated()]
    del all_data['date']
    all_data = all_data.groupby(['date', 'exchange']).sum().reset_index()
    all_data.index = all_data['date']
    del all_data['date']
    all_data.index = pd.to_datetime(all_data.index)
    return all_data




def get_btc_flows(TA_API_KEY):
    all_data =[]
    for exchange in get_exchanges():
        exch_inflows = get_data('btc', exchange, 'inflow', TA_API_KEY)
        exch_outflows = get_data('btc', exchange, 'outflow', TA_API_KEY)
        del exch_inflows['exchange']
        data = pd.concat([exch_inflows, exch_outflows], axis=1, join='inner')
        del data['date']
        all_data.append(data)

    return pd.concat(all_data).sort_index()


def load_price_data():
    csv_file = 'BTC.csv'
    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
    price_df = pd.read_csv(csv_file, index_col='date', parse_dates=True, date_parser=dateparse)

    return price_df['close']

def load_btc_flows():
    csv_file='btc_flows.csv'
    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
    flows_df = pd.read_csv(csv_file, index_col='date', parse_dates=True, date_parser=dateparse)
    return flows_df

def combine(price_df, flows_df):
    combined_df = pd.merge(price_df, flows_df, how='left',on=['date'])
    return combined_df

def process_df(combined_df):
    combined_df['net_flow'] = combined_df['outflow'] - combined_df['inflow']



if __name__=='__main__':
    TA_API_KEY='83cd86fae8d99a505c5dc33f27ed37e58af31d83872c23a5fa2b11a3013a8f97' #free api key
    TA_API_KEY = 'c946d24a771b1737b726db4c50f42ba60d6852081a0398257e6e9644a55f01ea' #david's key
    stablecoinflows_df = get_stablecoin_flows(TA_API_KEY)
    btcflows_df = get_btc_flows(TA_API_KEY)

    load_daily.update_historical(['BTC'])
    price_df = load_price_data()
    
    stablecoinflows_df = combine(price_df, stablecoinflows_df)
    btcflows_df = combine(price_df, btcflows_df)

    stablecoinflows_df.to_csv('stablecoin_flows.csv')
    btcflows_df.to_csv('bitcoin_flows.csv')
