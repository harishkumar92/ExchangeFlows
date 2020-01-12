import requests, sys
from datetime import datetime
import pandas as pd



def make_request(coin, toTs=None):
    ohlcv_url = 'https://min-api.cryptocompare.com/data/histoday?fsym={0}&tsym={1}&limit={2}'
    quote_pair = 'BTC' if coin != 'BTC' else 'USD'
    req_url = ohlcv_url.format(coin, quote_pair, 2000)
    if toTs:
        req_url = req_url + '&toTs=' + str(toTs)
    result = requests.get(req_url)
    return result


def process_result(result):
    if result.json()['Response'] != 'Success':
        print (result['Response'])

    data = pd.DataFrame(result.json()['Data'])
    data['date'] = pd.to_datetime(data.time.apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d')))
    data = data.set_index('date')
    return data


def save_df(coin, df):
    output_file = '{0}.csv'.format(coin)
    df.to_csv(output_file)


def update_with_latest(coin):
    csv_file = '{0}.csv'.format(coin)
    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    ohlcv_df = pd.read_csv(csv_file, index_col='date', parse_dates=True, date_parser=dateparse)

    latest_df = process_result(make_request(coin))

    first_time = latest_df.index.min()
    
    ohlcv_df = ohlcv_df[(ohlcv_df.index < first_time)]
    ohlcv_df = ohlcv_df.append(latest_df)
    save_df(coin, ohlcv_df)


def update_historical(coins):
    if coins == None:
        coins = get_coin_list(num_coins)

    for coin in coins:
        print ("Getting {0}...".format(coin))
        result = make_request(coin)
        df = process_result(result)
        save_df(coin, df)


if __name__ == '__main__':
    #update_with_latest(coin='BTC')
    update_historical(coins=['BTC'])
    pass



