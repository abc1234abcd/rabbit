import asyncio
from collections import deque
from kraken_ws import kraken_websocket_subscription
import pandas as pd
import numpy as np
import time
from datetime import datetime
'''
{'symbol': 'PEPE/USD', 'bid': 1.8266e-05, 'bid_qty': 1335900272.94374, 'ask': 1.8305e-05, 'ask_qty': 70700000.0, 
 'last': 1.8266e-05, 'volume': 3953930030188.7876, 'vwap': 1.766e-05, 'low': 1.2519e-05, 'high': 2.2479e-05, 
 'change': 4.755e-06, 'change_pct': 35.19}
'''
async def get_best_ob(queue):
    ticker_deque= deque(maxlen=3000)
    while True:
        try:
            best_ob_raw = await queue.get()
            for keys in best_ob_raw.items():
                if 'channel' in keys:
                    if best_ob_raw['channel'] == 'ticker':
                        for item in best_ob_raw['data']:
                            live_ticker ={
                                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-6] + "Z",
                                'bid': item['bid'],
                                'bid_qty': item['bid_qty'],
                                'ask': item['ask'],
                                'ask_qty': item['ask_qty'],
                                'last_trade_price': item['last'],
                                'change_pct_daily': item['change_pct'],
                                'change_daily': item['change'],
                                'low_daily': item['low'],
                                'high': item['high'],                                
                            }
                            ticker_deque.append(live_ticker)
                            ticker_df = pd.DataFrame(ticker_deque)
                            ticker_df['last_trade_price_pct'] = ticker_df['last_trade_price'].pct_change()
                            print(ticker_df)
        except Exception as e:
           print(f"pepe fun get best ob has error {e}")
async def main(topic):            
    queue = asyncio.Queue()
    kraken_instance = kraken_websocket_subscription(queue, topic)
    await asyncio.gather(
        kraken_instance.connect(),
        get_best_ob(queue)
    )

if __name__=='__main__': 
    topic =[{'method': 'subscribe', 'params': {'channel':'ticker', 'symbol': ["XRP/USD"], 'event_trigger': 'bbo'}}]
    asyncio.run(main(topic))