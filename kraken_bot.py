import asyncio
import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import requests
from kraken_ws import kraken_websocket_subscription
import json
import pandas as pd
import tkinter as tk
import threading

os.chdir(Path(__file__).parent.absolute())
logging.basicConfig(filename = f'kraken_bot_{datetime.now().strftime("%Y-%m-%d_%H_%M_%S")}.log', format = "%(asctime)s %(levelname)s-8s %(message)s", level=logging.INFO, datefmt = ("%Y-%m-%d %H:%M:%S"))


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
        return all_pairs_details, all_volumes
    except requests.RequestException as e:
        logging.error(f"Get Kraken tradable pair information failed {e}.")
           
def serialize_outputs():
    all_pairs_details, all_volumes = kraken_getTradablePair()
    with open('kraken_all_pairs_details.json', 'w') as file:
        json.dump(all_pairs_details, file)
    with open('kraken_all_volumes.json', 'w') as file:
        json.dump(all_volumes, file)

def kraken_find_big_volume_pair(n):    
    final_pairs_info ={}
    try:   
        with open('kraken_all_volumes.json', 'r') as file:
            kraken_all_volumes = json.load(file)
        with open('kraken_all_pairs_details.json', 'r') as file:
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
        with open('final_pairs_info.json', 'w') as file:
            json.dump(final_pairs_info, file)       
    except Exception as e:
        logging.error(f"an error occur in kraken_find_big_volume_pair {e}")
        raise f"an error occur in kraken_find_big_volume_pair function {e}."

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

async def get_raw_data(queue, big_market_trade_queue, best_ob_queue):
    big_market_trade = {}
    best_ob ={}
    while True:
        try:
            raw = await queue.get()
            for keys, values in raw.items():
                if 'channel' in keys:
                    if raw['channel'] == 'trade':
                        for item in raw['data']:
                            big_market_trade ={
                                "symbol": item['symbol'],
                                "timestamp": item['timestamp'],
                                "ord_type": item['ord_type'],
                                "side": item['side'],
                                "price": item['price'],
                                "qty": item['qty'],
                                "ord_volume": item['price']*item['qty'],
                            }                               
                            if big_market_trade['ord_volume'] >= 10000:
                                print(big_market_trade)                
                                await big_market_trade_queue.put(big_market_trade)
                    if raw['channel'] == 'ticker':
                        for item in raw['data']:
                            best_ob = {
                                "symobl": item['symbol'],
                                "best_bid": item['bid'],
                                "bid_qty": item['bid_qty']
                            }
                        await best_ob_queue.put(best_ob)
        except Exception as e:
            logging.error(f"kraken bot has error when get raw data{e}. ")
            
async def get_best_level_ob(big_market_trade_queue, best_ob_queue, message_label):
    prev_big_market_trade = {}
    while True: 
        try:
            best_level_ob_raw = await best_ob_queue.get()
            big_market_trade = await big_market_trade_queue.get_nowait()  
        except asyncio.QueueEmpty:
            big_market_trade = prev_big_market_trade   # as best_level_ob_raw will be refreshed way much quicker than the big market trades, the prev will be renewed as NONE as long as the 
            logging.info("Kraken bot get best level ob function the best ob queue so far is still empty, previous big market trade is displayed.")      
        except Exception as e:
            logging.error(f"kraken bot price monitor get best level ob function has error {e}.")
            big_market_trade = None
        message_label.config(text = f"{datetime.now()}\nLive monitor: {best_level_ob_raw} \nLarge market trade: {big_market_trade}") 
        prev_big_market_trade = big_market_trade
            

async def main(topic, message_label):
    queue=asyncio.Queue()
    big_market_trade_queue = asyncio.Queue()
    best_ob_queue = asyncio.Queue()
    kraken_instance = kraken_websocket_subscription(queue, topic)
    await asyncio.gather(
        kraken_instance.connect(),        
        get_raw_data(queue, big_market_trade_queue, best_ob_queue),
        get_best_level_ob(big_market_trade_queue, best_ob_queue, message_label),
    )

def windows_screen_asyncio_loop(topic, message_label):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(topic, message_label)) 

if __name__=='__main__':  
    kraken_find_big_volume_pair(n=400)
    with open('final_pairs_info.json', 'r') as file:
        final_pairs_info = json.load(file)
    print(len(final_pairs_info))    
    tickers = [item for item in final_pairs_info.keys() if item not in ["SOL/USD","USDC/USD"]]
    monitor_ticker = ["XRP/GBP","PEPE/USD", "TAO/USD"]
    topic =[{'method': 'subscribe', 'params': {'channel': 'trade', 'symbol': tickers, 'snapshot': False}},
            {'method': 'subscribe', 'params': {'channel':'ticker', 'symbol': monitor_ticker, 'event_trigger': 'bbo', 'snapshot': False}},
            ]
    root = tk.Tk()
    root.title("kraken trading monitor window")
    root.attributes('-topmost', True)
    message_label = tk.Label(root, text = "", font =("Helvetica", 16))
    message_label.pack(pady=20)
    threading.Thread(target = windows_screen_asyncio_loop, args = (topic, message_label), daemon = True).start()
    root.mainloop()

