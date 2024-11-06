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


def Kraken_getTradablePair(tickerA = None, tickerB = None):
    quote_usd_pairs =[]
    base_usd_pairs = []
    all_pairs =[]
    base_usd_counter =[]
    quote_usd_counter=[]
    x_targets =[]
    base_targets =[]
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
            if values['wsname'].startswith('USD/'):                
                base_usd_pairs.append(values['wsname'])
            if values['wsname'].endswith('/USD'):
                quote_usd_pairs.append(values['wsname'])    
        for item  in base_usd_pairs:
            base_counter = item.split('/')[1]
            base_usd_counter.append(base_counter)        
        for item in quote_usd_pairs:
            quote_counter = item.split('/')[0]
            quote_usd_counter.append(quote_counter) 
        for item_base_counter in base_usd_counter:
            for item_quote_counter in quote_usd_counter:
                looking_item_1 = f"{item_base_counter}/{item_quote_counter}" 
                looking_item_2 =f"{item_quote_counter}/{item_base_counter}"
                if looking_item_1 in all_pairs:
                    x_targets.append(looking_item_1)
                elif looking_item_2 in all_pairs:
                    x_targets.append(looking_item_2)
        for item in x_targets:
            temp_base = item.split('/')[0]
            base_targets.append(f"{temp_base}/USD")                       
        return base_usd_pairs, x_targets, base_targets
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

async def x_fx_arb_opportunity(ob_queue, x_targets):
    final_combo ={}
    while True:
        try:
            all_ob = await ob_queue.get()            
            for item in x_targets:                             
                temp_base = item.split('/')[0]
                temp_quote = item.split('/')[1]
                temp_1_symbol = f"USD/{temp_quote}"
                temp_3_symbol =f"{temp_base}/USD"                
                r_1 = float(all_ob[temp_1_symbol]['bid'])                
                r_1_qty = all_ob[temp_1_symbol]['bid_qty']                
                r_2 = float(all_ob[item]['ask'])               
                r_2_qty = all_ob[item]['ask_qty']
                r_3 = float(all_ob[temp_3_symbol]['bid'])                
                r_3_qty=all_ob[temp_3_symbol]['bid_qty']
                final_score = r_1*r_3/r_2*0.998**3                            
                final_combo[item] = { "pair_1": temp_1_symbol, "price_1": r_1, "qty_1": r_1_qty, "pair_2": item, "price_2": r_2, "qty_2": r_2_qty, "pair_3": temp_3_symbol, "price_3": r_3, "qty_3": r_3_qty, "final_score": final_score } 
            for keys, values in final_combo.items():
                if values['final_score'] > 0.993:
                    print(f"cross fx arbitrage opporunity seized, final score {values['final_score']} in pair {keys}!")        
        except Exception as e:
            logging.error(f"Error in x_fx_arb_opportunity: {e}")
             
async def main(topic, x_targets):
    queue = asyncio.Queue()
    ob_queue = asyncio.Queue()
    kraken_instance = kraken_websocket_subscription(queue, topic)
    await asyncio.gather(
        kraken_instance.connect(),
        get_ob_raw(queue, ob_queue),
        x_fx_arb_opportunity(ob_queue, x_targets)
    )

if __name__== '__main__':
    base_usd_pairs, x_targets, base_targets = Kraken_getTradablePair()   
    x_targets = [item.replace("XBT", "BTC") for item in x_targets]
    base_targets =[item.replace("XBT","BTC") for item in base_targets]
    #print(x_targets)
    tickers = base_usd_pairs + x_targets + base_targets 
    #print(tickers, len(tickers))
    topic =[{'method': 'subscribe', 'params': {'channel': 'ticker', 'symbol': tickers, 'event_trigger': 'bbo'}}]
    asyncio.run(main(topic, x_targets))