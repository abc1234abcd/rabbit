import logging
import os
from datetime import datetime
from pathlib import Path
import json
import requests
import pandas as pd
import requests
import time, base64, urllib, hashlib, hmac
from dotenv import load_dotenv
import asyncio
from kraken_ws import kraken_websocket_subscription
import threading
import ccxt 
from utils import mt, b_ob, candle, live_mt_deque
import plotly.graph_objects as go
from dataclasses import asdict, is_dataclass
from collections import deque, defaultdict, OrderedDict
from itertools import chain
from kraken_private_ws import kraken_private_ws_subscribe
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

os.chdir(Path(__file__).absolute().parent)
logging.basicConfig(filename =f'kraken_market_info_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.log', format = "%(asctime)s %(levelname)s-7s %(message)s",level = logging.INFO, datefmt = ("%Y_%m_%d_%H_%M_%S"))

def kraken_getTradablePair(tickerA = None, tickerB = None):
    all_pairs_details = {}
    all_volumes = {}    
    try:
        params ={}
        if tickerA and tickerB:
            params = {'pair': tickerA +'/'+ tickerB}
        tradablePairResp = requests.get('https://api.kraken.com/0/public/AssetPairs', params = params)        
        if tradablePairResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken tradable pair failed {tradablePairResp.status_code}.")
        tradablePair = tradablePairResp.json()['result']                  
        for keys, values in tradablePair.items():            
            all_pairs_details[values['wsname']] = {"altname": values['altname'], "fees": list(values['fees'])[0][1]} 
        for keys, values in all_pairs_details.items():
            params={'pair': f"{values['altname']}"}
            tradablePairInfoResp = requests.get("https://api.kraken.com/0/public/Ticker", params = params)   
            if tradablePairInfoResp.status_code != 200:
                logging.error(f"getting tradable pair information has error {tradablePairInfoResp.status_code}.") 
                raise f"an error occur while getting tradable pair information {tradablePairInfoResp.status_code}" 
            tradablePairInfo = tradablePairInfoResp.json()['result']
            for keys, values in tradablePairInfo.items():
                all_volumes[keys]= values['t']                                     
        with open(f'kraken_all_pairs_details_{datetime.now().strftime("%Y_%m_%d")}.json', 'w') as file:
            json.dump(all_pairs_details, file)
        with open(f'kraken_all_volumes_{datetime.now().strftime("%Y_%m_%d")}.json', 'w') as file:
            json.dump(all_volumes, file)
    except requests.RequestException as e:
        logging.error(f"Get Kraken all tradable pair in kraken_getTradablePair function failed {e}.")          
def kraken_find_big_volume_pair(n):  
    kraken_getTradablePair()  
    final_pairs_info ={}
    try:   
        with open(f'kraken_all_volumes_{datetime.now().strftime("%Y_%m_%d")}.json', 'r') as file:
            kraken_all_volumes = json.load(file)
        with open(f'kraken_all_pairs_details_{datetime.now().strftime("%Y_%m_%d")}.json', 'r') as file:
            kraken_all_pairs_details = json.load(file)  
        volume_df_temp = pd.DataFrame(kraken_all_volumes)
        volume_df_temp.index = ['today_volume', 'past_24hr_volume']
        volume_df_t = volume_df_temp.T        
        sorted_volume_df = volume_df_t.sort_values(by = ['past_24hr_volume', 'today_volume'])
        chosen_df = sorted_volume_df.iloc[-n:] 
        chosen_df_index = chosen_df.index
        chosen_usd_pairs = [item for item in chosen_df_index if item.endswith('USD')]  
        for keys, values in kraken_all_pairs_details.items():
            for item in chosen_usd_pairs:
                if item == values['altname']:
                    final_pairs_info[keys] = values               
        with open(f'final_pairs_info_{datetime.now().strftime("%Y_%m_%d")}.json', 'w') as file:
            json.dump(final_pairs_info, file)       
    except Exception as e:
        logging.error(f"an error occur in kraken_find_big_volume_pair {e}")
def krakenApiSign(urlpath, data, krakenapisecret):    
    if isinstance(data, str):
        encoded = (str(json.loads(data)["nonce"]) + data).encode()
    else:
        encoded = (str(data["nonce"]) + urllib.parse.urlencode(data)).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(krakenapisecret), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()
def kraken_get_websocket_token(krakenapikey, krakenapisecret):    
    urlpath = "/0/private/GetWebSocketsToken"
    url = "https://api.kraken.com"+ urlpath
    data = {'nonce': str(int(time.time()*1000))}  
    apisignature =krakenApiSign(urlpath, data, krakenapisecret)
    headers = {
        'API-KEY': krakenapikey,
        'API-Sign': apisignature,
    }
    resp = requests.post(url, headers = headers, data = data)    
    if resp.status_code != 200:
        logging.error(f"kraken market info get websocket token has error {resp.status_code}")
    else:
        token = resp.json()['result']['token']
    return token  
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
        accountBalance = [ [key, value] for key, value in accountBalanceResp.json()['result'].items() if float(value) != 0]                     
        return accountBalance
    except requests.RequestException as e:
        logging.error(f"Get Kraken account balance failed {e}.")       
def add_rsi(data):
    window = 14
    delta = data['close'].astype(float).diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss.replace(0, pd.NA)
    data['rsi'] = 100 - (100 / (1 + rs))
    data['rsi'] = data['rsi'].ffill().infer_objects()
    return data
def get_ohlc(tickers, interval):
    ohlc_df_dict = {}    
    try:
        for ticker in tickers:       
            for element in interval:
                params ={'pair': ticker, 'interval': element}
                ohlcResp = requests.get('https://api.kraken.com/0/public/OHLC', params = params)
                if ohlcResp.status_code != 200:
                    raise requests.HTTPError(f"Get Kraken OHLC failed {ohlcResp.status_code}.")
                ohlc = ohlcResp.json()['result']          
                columns = ['time', 'open','high','low','close','vwap','volume','count']
                ohlc_df = pd.DataFrame(ohlc[ticker], columns = columns)    
                ohlc_df[['open','high','low','close','vwap','volume','count']].astype(float)            
                ohlc_df['timestamp'] = pd.to_datetime(ohlc_df['time'], unit='s')
                ohlc_df.set_index('timestamp', inplace=True)
                ohlc_df = add_rsi(ohlc_df)
                ohlc_df['close_pct'] = ohlc_df['close'].astype(float).pct_change()
                ohlc_df.dropna(inplace=True)
                dict_name = f"{ticker}{element}"
                ohlc_df_dict[dict_name]= ohlc_df                                             
        return ohlc_df_dict
    except requests.RequestException as e:
        logging.error(f"Get Kraken OHLC failed {e}.")          
        
##live## 

async def get_kraken_live_raw_pub(db, queue, live_ob_queue, live_mt_queue, live_ohlc_queue):    
    while True:
        try:
            raw = await queue.get()            
            for keys in raw.items():               
                if 'channel' in keys:
                    if raw['channel'] == 'trade':       
                        collection = db['market_trades']            
                        for item in raw['data']:                         
                            market_trade = mt(  
                                ticker=item['symbol'],                           
                                timestamp = item['timestamp'],
                                type=item['ord_type'],
                                side= item['side'],
                                price= item['price'],
                                qty= item['qty'],
                                volume= item['price']*item['qty'])
                            market_trades_dict =asdict(market_trade)
                            collection.insert_one(market_trades_dict)
                            await live_mt_queue.put(market_trade)         
                    elif raw['channel'] == 'ticker': 
                        collection = db['ticker']                                            
                        for item in raw['data']:
                            best_ob = b_ob(
                                ticker = item['symbol'],
                                timestamp= datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),                         
                                bids= [item['bid'], item['bid_qty']],                                
                                asks=[item['ask'],item['ask_qty']], 
                                last_price = item['last'],
                                day_high = item['high'],
                                day_low = item['low'],
                                day_pct = item['change_pct'],
                                day_volume =item['volume'],
                                day_vwap = item['vwap']) 
                            best_ob_dict =asdict(best_ob)
                            collection.insert_one(best_ob_dict)                                                 
                            await live_ob_queue.put(best_ob)
                    elif raw['channel'] == 'ohlc':   
                        collection = db['candle']                                             
                        for item in raw['data']:
                            ohlc = candle(
                                ticker = item['symbol'],
                                timestamp = item['interval_begin'],
                                open=item['open'],
                                high=item['high'],
                                low= item['low'],
                                close= item['close'],
                                no_trades = item['trades'],
                                volume = item['volume'],
                                vwap = item['vwap'])
                            ohlc_dict =asdict(ohlc)
                            collection.insert_one(ohlc_dict)
                        await live_ohlc_queue.put(ohlc)
        except Exception as e:
            logging.error(f"kraken market information has error in get raw data function {e}.")   
            
async def get_kraken_live_raw_pri(db, queue_pri):
    ob_pri = defaultdict(lambda: defaultdict())
    while True:  
        try:
            live_raw_pri = await queue_pri.get()
            if live_raw_pri:
                if 'channel' in live_raw_pri:
                    if live_raw_pri['channel'] == 'level3':
                        collection = db['private_ob']
                        if live_raw_pri['type'] == 'snapshot':
                            snapshot_data = live_raw_pri['data']
                            for element in snapshot_data:
                                temp_ticker = element['symbol']
                                bids_item ={}
                                asks_item ={}
                                for item in element['bids']:  
                                    bids_name = item['order_id'] 
                                    bids_item[bids_name] = {
                                        'limit_price': item['limit_price'],
                                        'order_qty': item['order_qty'],
                                        'timestamp': item['timestamp'],
                                    }   
                                for item in element['asks']:
                                    asks_name = item['order_id']
                                    asks_item[asks_name] ={
                                        'limit_price': item['limit_price'],
                                        'order_qty': item['order_qty'],
                                        'timestamp':item['timestamp'],
                                    }
                                ob_pri[temp_ticker]['bids'] = bids_item
                                ob_pri[temp_ticker]['asks'] = asks_item                              
                                ob_pri[temp_ticker]['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                                collection.insert_one({temp_ticker: ob_pri[temp_ticker]})
                        elif live_raw_pri['type'] == 'update':
                            update_data = live_raw_pri['data']
                            for element in update_data:
                                update_ticker = element['symbol']
                                if update_ticker in ob_pri:
                                    book_toModified = ob_pri[update_ticker]
                                    for item in element['bids']:
                                        if item:
                                            bids_name= item['order_id']
                                            if item['event'] == 'add':
                                                book_toModified['bids'][bids_name] = {                                                                                    
                                                'limit_price': item['limit_price'],
                                                'order_qty': item['order_qty'],
                                                'timestamp': item['timestamp']
                                            }
                                            elif item['event'] =='delete':
                                                del book_toModified['bids'][bids_name] 
                                    for item in element['asks']:
                                        if item:
                                            asks_name =item['order_id']
                                            if item['event'] == 'add':
                                                book_toModified['asks'][asks_name] ={
                                                    'limit_price': item['limit_price'],
                                                    'order_qty': item['order_qty'],
                                                    'timestamp': item['timestamp'],
                                                }
                                            elif item['event'] == 'delete':
                                                del book_toModified['asks'][asks_name] 
                                    
                                book_toModified['timestamp'] =  datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                                ob_pri[update_ticker] = book_toModified
                                
                                print('update private ob')
                                print({update_ticker: ob_pri[update_ticker]})   
                                collection.insert_one({update_ticker: ob_pri[update_ticker]})                                   
        except Exception as e:
            logging.error(f"kraken market information get live raw private has error:{e}.")
            
async def get_b_ob_book(live_ob_queue): 
    b_ob_book_all =defaultdict(lambda: deque(maxlen=100))
    df_dict ={}
    columns = ['ticker','timestamp', 'bids', 'bids_qty', 'asks', 'asks_qty', 'last_price', 'day_high', 'day_low', 'day_pct', 'day_volume', 'day_vwap']
    while True:   
        try:
            b_ob_raw = await live_ob_queue.get()
            temp_ticker = b_ob_raw.ticker
            temp_data = [b_ob_raw.ticker, b_ob_raw.timestamp,b_ob_raw.bids, b_ob_raw.asks,b_ob_raw.last_price,b_ob_raw.day_high, b_ob_raw.day_low,b_ob_raw.day_pct,b_ob_raw.day_volume,b_ob_raw.day_vwap]
            final_data =[item for sublist in temp_data for item in (sublist if isinstance(sublist, list) else [sublist])]    
        except Exception as e:
            logging.error(f"kraken market info has error in b_ob_boook {e}.")
    
async def get_mt_book(live_mt_queue): 
    live_raw = defaultdict(lambda: OrderedDict())
    while True:
        try:
            live_mt_raw = await live_mt_queue.get() 
            if live_mt_raw:
                temp_ticker = live_mt_raw.ticker 
                temp_mins = live_mt_raw.minutes
                temp_data = [live_mt_raw.timestamp, live_mt_raw.type, live_mt_raw.side, live_mt_raw.price, live_mt_raw.qty, live_mt_raw.volume]
                if temp_ticker not in live_raw:
                    live_raw[temp_ticker] = OrderedDict()
                if temp_mins not in live_raw[temp_ticker]:
                    live_raw[temp_ticker][temp_mins] = []
                live_raw[temp_ticker][temp_mins].append(temp_data)
                if len(live_raw[temp_ticker]) > 2:
                    live_raw[temp_ticker].popitem(last=False)           
        except Exception as e:
            logging.error(f"kraken market information get mt book has error {e}.") 
            
async def find_market_toEnter():
    while True:
        try:
          print("passby")  
        except Exception as e:
            logging.eror(f"kraken market information find market to enter has error {e}.")            
                         
async def main(topic_pub, topic_pri, db):
    queue_pub =asyncio.Queue()
    queue_pri = asyncio.Queue()
    live_ohlc_queue = asyncio.Queue()
    live_mt_queue = asyncio.Queue()
    live_ob_queue = asyncio.Queue()
    live_ob_book_queue = asyncio.LifoQueue()
    kraken_instance_pub = kraken_websocket_subscription(queue_pub, topic_pub)
    kraken_instance_pri = kraken_private_ws_subscribe(queue_pri, topic_pri)
    await asyncio.gather(
        kraken_instance_pub.connect(),
        kraken_instance_pri.connect(),
        get_kraken_live_raw_pub(db, queue_pub, live_ob_queue, live_mt_queue, live_ohlc_queue),
        #get_kraken_live_raw_pri(db, queue_pri),
        #get_b_ob_book(live_ob_queue),
        #get_mt_book(live_mt_queue),
    )       
        
if __name__=='__main__':  
    pd.set_option('future.no_silent_downcasting', True)
    load_dotenv()
    krakenapikey = os.getenv("KRAKENAPIKEY")
    krakenapisecret =os.getenv("KRAKENSECRET")  
    kraken_ws_token=kraken_get_websocket_token(krakenapikey, krakenapisecret) 
    mongo_url = os.getenv("MONGOATLAS")
    mongo_client = MongoClient(mongo_url)
    db = mongo_client['kraken']
    kraken_find_big_volume_pair(n=400)
    with open(f'final_pairs_info_{datetime.now().strftime("%Y_%m_%d")}.json', 'r') as file:
        final_pairs_info = json.load(file)
    tickers = [item for item in final_pairs_info.keys() if item not in ["SOL/USD","USDC/USD"]]
    interval = [1]
    topic_pub =[{'method': 'subscribe', 'params': {'channel': 'trade', 'symbol': tickers}},          
                #{'method': 'subscribe', 'params': {'channel': 'ohlc', 'symbol': tickers, 'interval': 1}},
                {'method': 'subscribe', 'params': {'channel':'ticker', 'symbol': tickers, 'event_trigger': 'bbo'}},
                #{'method': 'subscribe', 'params': {'channel': 'book', 'symbol': tickers}}
    ]   
    topic_pri = [{'method': 'subscribe', 'params': {'channel': 'level3', 'symbol': tickers,'token': kraken_ws_token}}]
    asyncio.run(main(topic_pub, topic_pri, db))
        
                                                                                  
'''    
    hist_candle = get_ohlc(tickers, interval)
    for key, value in hist_candle.items():
        min_pct_close = value['close_pct'].min()
        min_pct_position = value['close_pct'].idxmin()
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=value.index, 
            y=value['close'].astype(float), 
            mode='markers',
            name=key                       
        ))
        fig.add_trace(go.Scatter(
            x=[min_pct_position],
            y=[min_pct_close],
            mode='markers',
            marker=dict(color='red', size=10),
            name='min pct'
        ))
        fig.add_shape(
            type = 'line',
            x0=min_pct_position,
            x1= min_pct_position,
            y0= value['close'].min(),
            y1=value['close'].max(),
            line=dict(color='red', width=2, dash='dot'),
            name = 'min line'
        )
        fig.update_layout(
            title=f"Minimum Close Percentage for {key}",
            xaxis_title="Timestamp",
            yaxis_title="Close",
        )
        fig.show()
'''   

