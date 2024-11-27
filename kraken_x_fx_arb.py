import asyncio
import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from kraken_ws import kraken_websocket_subscription
import requests


os.chdir(Path(__file__).parent.absolute())
logging.basicConfig(filename = f'ccxt_x_fx_arb_{datetime.now().strftime("%Y-%m-%d_%H_%M_%S")}.log', format = "%(asctime)s %(levelname)s -8s %(message)s", level=logging.INFO, datefmt = "%Y-%m-%d %H:%M:%S")

def Kraken_getTradablePair(cash_currency, tickerA = None, tickerB = None):
    quote_usd_pairs =[]
    all_pairs =[]
    base_all = []
    trail = []
    final = []
    final_cash_currency =[]
    try:
        params ={}
        if tickerA and tickerB:
            params = {'pair': tickerA +'/'+ tickerB}
        tradablePairResp = requests.get('https://api.kraken.com/0/public/AssetPairs', params = params)        
        if tradablePairResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken tradable pair failed {tradablePairResp.status_code}.")
        tradablePair = tradablePairResp.json()['result']   
        for keys, values in tradablePair.items():
            all_pairs.append(values['wsname'])                      
            if values['wsname'].endswith('/USD'):
                quote_usd_pairs.append(values['wsname'])   
        for item in cash_currency:
            if item != 'USD':
                cash_trail_1 = item + "/USD"
                cash_trail_2 = "USD/" + item
                if cash_trail_1 in all_pairs:
                    trail.append(cash_trail_1)
                    final_cash_currency.append(item)                       
                elif cash_trail_2 in all_pairs:
                    trail.append(cash_trail_2)
                    final_cash_currency.append(item)
        for i in range(len(final_cash_currency)):
            for j in range(1, len(final_cash_currency)):
                trail_1 = final_cash_currency[i]+ "/" + final_cash_currency[j]
                trail_2 = final_cash_currency[j]+ "/" + final_cash_currency[i]
                if trail_1 in all_pairs:
                    final.append(trail_1)
                elif trail_2 in all_pairs:
                    final.append(trail_2)                             
        return list(set(final)), trail
    except requests.RequestException as e:
        logging.error(f"Get Kraken tradable pair information failed {e}.")
        
async def get_ob_raw(queue, ob_queue):
    all_pairs_ob={}
    while True:  
        try:
            ob_raw_temp = await queue.get()            
            if 'channel' in ob_raw_temp and ob_raw_temp['channel'] == 'ticker':                       
                for item in ob_raw_temp['data']:
                    id = item['symbol']
                    all_pairs_ob[id] = item                                                              
                await ob_queue.put(all_pairs_ob)                   
        except Exception as e:
            logging.error(f"Error in get_ob_raw: {e}")

async def x_fx_arb_opportunity(ob_queue, tickers):
    final_combo ={}
    while True:
        try:
            all_ob = await ob_queue.get()            
            for item in tickers:
                if "USD/" not in item and "/USD" not in item:                                       
                    up = item.split('/')[0]                
                    down = item.split('/')[1]
                    up_ticker = up + '/USD'
                    down_ticker = down + '/USD'
                    if up_ticker in tickers:
                        if down_ticker in tickers:
                            up_bid = all_ob[up_ticker]['bid']
                            up_ask = all_ob[up_ticker]['ask']
                            
                            down_bid = all_ob[down_ticker]['bid']
                            down_ask = all_ob[down_ticker]['ask']
                            
                            mid_bid = all_ob[item]['bid']
                            mid_ask = all_ob[item]['ask']
                            
                            score_one = mid_bid*down_bid*0.9992**3/up_ask
                            score_two = mid_ask*up_ask*0.9993**3/down_ask
                            final_score = max([score_one, score_two])
                            print(item, final_score)
                        
                   
                    
                #final_score = r_1*r_3/r_2*0.9992**3                            
                #final_combo[item] = { "pair_1": temp_1_symbol, "price_1": r_1, "qty_1": r_1_qty, "pair_2": item, "price_2": r_2, "qty_2": r_2_qty, "pair_3": temp_3_symbol, "price_3": r_3, "qty_3": r_3_qty, "final_score": final_score } 
            #for keys, values in final_combo.items():
               # if values['final_score'] > 0.998:
                    #print(f"cross fx arbitrage opporunity seized, final score {values['final_score']} in pair {keys}!")        
        except Exception as e:
            logging.error(f"Error in x_fx_arb_opportunity: {e}")
             
async def main(topic, tickers):
    queue = asyncio.Queue()
    ob_queue = asyncio.Queue()
    kraken_instance = kraken_websocket_subscription(queue, topic)
    await asyncio.gather(
        kraken_instance.connect(),
        get_ob_raw(queue, ob_queue),
        x_fx_arb_opportunity(ob_queue, tickers)
    )

if __name__== '__main__':
    cash_currency= ['USD','EUR','CAD','AUD','GBP','CHF','JPY', 'USDT', 'USDC'] 
    mid, target = Kraken_getTradablePair(cash_currency = cash_currency)
    tickers = mid + target
    topic =[{'method': 'subscribe', 'params': {'channel': 'ticker', 'symbol': tickers, 'event_trigger': 'bbo'}}]
    asyncio.run(main(topic, tickers))