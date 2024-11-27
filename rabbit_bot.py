import asyncio
import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
import requests
from kraken_ws import kraken_websocket_subscription
import json
import pandas as pd
import urllib, hashlib, hmac, base64, time
from telegram import Bot
from collections import deque
from utils import live_market_trades_deque, live_ohlc_deque
from itertools import islice

'''

1. DONE. run kraken_market_info.py first, to get the first n largest trading volume pair information.
as the market trade pair is ranked based on past 24 hours trading volume. this script is better to be updated every 24 hours.

2. after large buy order, check out the price movement afterwards and see if there is any buy point to enter the market.

3. enter market point, has to be sorted out as i always enter in a very bad position.

4. the depth of 2-3 price level can help to figure out if i need to clear the position or not.

5. rsi strategy for a stable market e.g xrp, pepe

6. try to see dex flash loan and sanwitch attack.

'''

os.chdir(Path(__file__).absolute().parent)
logging.basicConfig(filename = f'rabbit_bot_{datetime.now().strftime("%Y-%m-%d_%H_%M_%S")}.log', format = "%(asctime)s %(levelname)s-7s %(message)s", level=logging.INFO, datefmt = ("%Y-%m-%d %H:%M:%S"))
#---------------------restapi---------------------#
def krakenApiSign(urlpath, data, secret):    
    if isinstance(data, str):
        encoded = (str(json.loads(data)["nonce"]) + data).encode()
    else:
        encoded = (str(data["nonce"]) + urllib.parse.urlencode(data)).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()
def getAccBalance(krakenapikey, krakenapisecret):    
    try:
        data = {'nonce': str(int(time.time()*1000))}  
        urlpath = '/0/private/Balance'              
        url = 'https://api.kraken.com' + urlpath
        apisignature =krakenApiSign(urlpath, data, krakenapisecret)        
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,
        }
        accountBalanceResp = requests.post(url, headers=headers, data = data)
        if accountBalanceResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken account balance failed {accountBalanceResp.status_code}.")
        accountBalance = accountBalanceResp.json()['result']        
        return accountBalance
    except requests.RequestException as e:
        logging.error(f"Get Kraken account balance failed {e}.")
def getRecentTrades(ticker):
    try:        
        params = {'pair': ticker, 'count': 1000}
        recentTradesResp = requests.get("https://api.kraken.com/0/public/Trades", params = params)
        if recentTradesResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken recent trades failed {recentTradesResp.status_code}.")
        recentTrades = recentTradesResp.json()['result']             
        return recentTrades
    except requests.RequestException as e:
        logging.error(f"Get Kraken recent trades failed {e}.")
def getohlc_hist(tickers):
    ohlc = {}
    intervals = [1, 30, 60, 1440]   
    try:
        for ticker in tickers:
            for interval in intervals:
                params ={'pair': ticker, 'interval': interval}
                ohlcResp = requests.get('https://api.kraken.com/0/public/OHLC', params = params)
                if ohlcResp.status_code != 200:
                    logging.error(f"Get kraken OHLC failed {ohlcResp.status_code}.")
                    raise requests.HTTPError(f"Get Kraken OHLC failed {ohlcResp.status_code}.")
                ohlc_temp = ohlcResp.json()['result']
                ohlc[f"{ticker}_{interval}"] = ohlc_temp  
        with open('ohlc.json', 'w') as file:
            json.dump(ohlc, file)     
    except requests.RequestException as e:
        logging.error(f"Get Kraken OHLC failed {e}.")
def getRsi(data):
    window = 14
    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss.replace(0, pd.NA)
    data['rsi'] = 100 - (100 / (1 + rs))
    data['rsi'] = data['rsi'].ffill().infer_objects()
    return data
def ohlc_rsi_index():
    with open('ohlc.json', 'r') as file:
        ohlc_raw_data = json.load(file)
    symbol = next(iter(ohlc_raw_data.keys()))
    ohlc_raw = pd.DataFrame(ohlc_raw_data[symbol])
    ohlc_raw.columns = ['time','open','high','low', 'close','vwap', 'volume','count']
    ohlc_raw[['time','open','high','low', 'close','vwap', 'volume','count']] = ohlc_raw[['time','open','high','low', 'close','vwap', 'volume','count']].apply(pd.to_numeric, errors = 'coerece')
    ohlc_raw['timestamp'] = pd.to_datetime(ohlc_raw['time'], unit ='s')
    ohlc_raw.set_index('timestamps', inplace =True)    
    return ohlc_raw
#---------------------websocket-------------------#
async def get_live_raw(queue, live_ob_queue, live_mt_queue, live_ohlc_queue):    
    while True:
        try:
            raw = await queue.get()            
            for keys in raw.items():               
                if 'channel' in keys:
                    if raw['channel'] == 'trade':                   
                        for item in raw['data']:                         
                            market_trade = {   
                                'symbol': item['symbol'],                           
                                'timestamp': item['timestamp'],
                                'ord_type': item['ord_type'],
                                'side': item['side'],
                                'price': item['price'],
                                'qty': item['qty'],
                                'ord_volume': item['price']*item['qty'],
                            }
                            await live_mt_queue.put(market_trade)         
                    elif raw['channel'] == 'ticker':                                             
                        for item in raw['data']:
                            best_ob = {
                                "timestamp": time.time()*1000,
                                "symobl": item['symbol'],
                                "bid": item['bid'],
                                "bid_qty": item['bid_qty'],
                                "ask": item['ask'],
                                "ask_qty": item['ask_qty'],
                                "last_trade_price": item['last'],
                                "low_past_24hrs": item['low'],
                                "high_past_24hrs": item['high'],
                                "change_pct_24hrs": item['change_pct'],
                                "volume_currency_past_24hrs": item['volume'],
                                "vwap_past_24hrs": item['vwap'],
                            }                                                   
                            await live_ob_queue.put(best_ob)
                    elif raw['channel'] == 'ohlc':                                                
                        for item in raw['data']:
                            ohlc = {
                                'symbol': item['symbol'],
                                'open': item['open'],
                                'high':item['high'],
                                'low': item['low'],
                                'close': item['close'],
                                'trades_number': item['trades'],
                                'volume_ohlc': item['volume'],
                                'vwap_ohlc': item['vwap'],
                                'timestamp': item['interval_begin'],                              
                            }
                        await live_ohlc_queue.put(ohlc)
        except Exception as e:
            logging.error(f"rabbit bot has error in get raw data function {e}.")    
async def get_mt_dict_raw(tickers, live_mt_queue):
    all_ticker_live_mt = {}
    mt_df ={}
    final_data ={}
    columns_mt = ['timestamp', 'ord_type', 'side', 'price','qty', 'ord_volume']
    final_output = {}
    for item in tickers:
        all_ticker_live_mt[item] = live_market_trades_deque(item)  
    while True:
        try:
            market_trade_raw = await live_mt_queue.get()
            if market_trade_raw:
                temp_ticker = market_trade_raw['symbol']
                temp_data = [market_trade_raw['timestamp'],market_trade_raw['ord_type'],market_trade_raw['side'],market_trade_raw['price'],market_trade_raw['qty'],market_trade_raw['ord_volume']]
                if temp_ticker in all_ticker_live_mt:         
                    all_ticker_live_mt[temp_ticker].data.append(temp_data)
                else:
                    all_ticker_live_mt[temp_ticker] = live_market_trades_deque(temp_ticker)
                    all_ticker_live_mt[temp_ticker].data.append(temp_data)
                for key, value in all_ticker_live_mt.items():
                    if value.data:
                        temp_df = pd.DataFrame(value.data, columns = columns_mt)
                        temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
                        temp_df['time_delta'] = temp_df['timestamp'].diff().dt.total_seconds()
                        temp_df.loc[temp_df['time_delta'] == 0,'time_delta'] =1
                        temp_df['pct'] = temp_df['price'].astype(float).pct_change()
                        temp_df['pct_speed'] = temp_df['pct']/temp_df['time_delta'] 
                        mt_df[key] = temp_df
                for keys, values in mt_df.items():
                    print(keys)
                    print(values)
                    if values:
                        values['timestamp'] = pd.to_datetime(values['timestamp'])
                        values['minute'] = values['timestamp'].dt.floor('min')
                        grouped_df = values.data.groupby('minute')
                        latest_minute = values.data['minute'].max()
                        latest_minute_group = grouped_df.get_group(latest_minute)
                        final_temp = [
                            latest_minute,
                            latest_minute_group['pct'].sum() if 'pct' in latest_minute_group else None,
                            latest_minute_group['price'].iloc[-1] if not latest_minute_group.empty else None,
                            latest_minute_group['ord_volume'].iloc[-1] if not latest_minute_group.empty else None,
                            latest_minute_group['side'].iloc[-1] if not latest_minute_group.empty else None,
                            latest_minute_group['ord_type'].iloc[-1] if not latest_minute_group.empty else None
                        ] 
                        final_data[keys] = final_temp 
                        final_latest_data = {key: value for key, value in sorted(final_data.items(), key=lambda item: item[1][1], reverse=False)}  
                        print('final data')   
                        print(dict(islice(final_latest_data.items(), 10)))
        except Exception as e:
            logging.error(f"rabbit bot get_mt_dict_raw has error {e}.")             
def add_rsi(data):
    window = 14
    delta = data['close'].astype(float).diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss.replace(0, pd.NA)
    data['rsi'] = 100 - (100 / (1 + rs))
    data['rsi'] = data['rsi'].ffill().infer_objects()
    return data      
async def live_ohlc(tickers, live_ohlc_queue):
    live_ohlc_dict = {}
    live_ohlc_df = {}
    ohlc_columns = ['timestamps', 'open','high','low', 'close','trades_number', 'volume_ohlc']
    for item in tickers:
        live_ohlc_dict[item] = live_ohlc_deque(item)
    while True:   
        try:
            live_ohlc_raw = await live_ohlc_queue.get()            
            if live_ohlc_raw:
                symbol = live_ohlc_raw['symbol']
                data_temp = [live_ohlc_raw['timestamp'], live_ohlc_raw['open'], live_ohlc_raw['high'],live_ohlc_raw['low'],live_ohlc_raw['close'], live_ohlc_raw['trades_number'], live_ohlc_raw['volume_ohlc']]
                if symbol in live_ohlc_dict:
                    live_ohlc_dict[symbol].data.append(data_temp)
                else:
                    live_ohlc_dict[symbol] = live_ohlc_deque(symbol)
                    live_ohlc_dict[symbol].data.append(data_temp)
                for key, value in live_ohlc_dict.items():
                    if value.data:
                        temp_df = pd.DataFrame(value.data, columns = ohlc_columns)
                        df = add_rsi(temp_df)
                        live_ohlc_df[key] = df
        except Exception as e:
            logging.error(f"rabbit bot live ohlc function has error {e}.")
async def op(live_ob_queue, live_mt_queue):
    ob_columns = ['timestamp', 'bid', 'bid_qty','ask','ask_qty','last_trade','daily_low','daily_high','daily_change','daily_volume','daily_vwap']
    mt_columns = ['timestamp', 'ord_type', 'side', 'price','qty', 'ord_volume']
    mt_dict = {}
    for ticker in tickers:
        mt_dict[ticker] = live_market_trades_deque(ticker)  
    while True:  
        try:
            best_ob_raw = await live_ob_queue.get()
            if best_ob_raw:
                print(best_ob_raw)
        except Exception as e:
            logging.error(f"kraken bot operation function has error {e}.")
async def main(topic, tickers, krakenapikey, krakenapisecret):
    queue=asyncio.Queue()
    market_trade_queue = asyncio.Queue()
    live_ob_queue = asyncio.Queue()
    live_ohlc_queue = asyncio.Queue()
    live_mt_queue = asyncio.Queue()
    kraken_instance = kraken_websocket_subscription(queue, topic)
    await asyncio.gather(
        kraken_instance.connect(),        
        get_live_raw(queue, live_ob_queue, live_mt_queue, live_ohlc_queue),
        get_mt_dict_raw(tickers, market_trade_queue),
        live_ohlc(tickers, live_ohlc_queue),
    )  
if __name__=='__main__': 
    krakenapikey = 1#os.getenv('KRAKENAPIKEY')
    krakenapisecret = 2#os.getenv('KRAKENAPISECRET')
    #bot_token = os.getenv("BOTFATHER")
    #chat_id = os.getenv("CHATID")
    #bot = Bot(token=bot_token)    
    with open(f'final_pairs_info_{datetime.now().strftime("%Y_%m_%d")}.json', 'r') as file:
        final_pairs_info = json.load(file)    
    #tickers = [item for item in final_pairs_info.keys() if item not in ["SOL/USD","USDC/USD"]]
    tickers = ["XRP/USD"]
    topic =[{'method': 'subscribe', 'params': {'channel': 'trade', 'symbol': tickers}},
            #{'method': 'subscribe', 'params': {'channel':'ticker', 'symbol': tickers, 'event_trigger': 'bbo'}},
            #{"method": "subscribe", "params": {"channel": "balances", "token": "G38a1tGFzqGiUCmnegBcm8d4nfP3tytiNQz6tkCBYXY"}},            
            #{'method': 'subscribe', 'params': {'channel': 'ohlc', 'symbol': tickers, 'interval': 1, 'snapshot': True}},
    ]
    asyncio.run(main(topic, tickers, krakenapikey, krakenapisecret))
    
