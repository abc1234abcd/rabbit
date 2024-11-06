import asyncio
import tkinter as tk
import logging
import os
from pathlib import Path
from datetime import datetime
from kraken_ws import kraken_websocket_subscription
import threading

os.chdir(Path(__file__).parent.absolute())
logging.basicConfig(filename = f'kraken_price_monitor_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.log', format  = "%(asctime)s %(levelname)s-8s %(message)s", level=logging.INFO, datefmt = "%Y_%m_%d_%H_%M_%S")


async def get_best_level_ob(queue, message_label):
    while True: 
        try:
            best_level_ob_raw = await queue.get()
            for keys, values in best_level_ob_raw.items():
                if 'channel' in keys and best_level_ob_raw['channel'] =='ticker':
                    ob_raw_temp = best_level_ob_raw['data']
                    for item in ob_raw_temp:
                        best_bid= item['bid']
                        bid_qty = item['bid_qty']            
                        message_label.config(text = f"{datetime.now()} best bid: {best_bid}, qty: {bid_qty}")
                        if best_bid >= 0.3972:
                            message_label.config(text = f"{datetime.now()} !EXECUTION! best bid: {best_bid}, qty: {bid_qty}")
        except Exception as e:
            logging.error(f"kraken price monitor get best level ob function has error {e}.")
            raise f"kraken price monitor get best level ob function has error {e}."

async def main(topic, message_label):
    queue = asyncio.Queue()
    kraken_instance = kraken_websocket_subscription(queue, topic)
    await asyncio.gather(
        kraken_instance.connect(),
        get_best_level_ob(queue, message_label),
    )

def windows_screen_asyncio_loop(topic, message_label):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(topic, message_label)) 
      
if __name__ =='__main__':
    tickers = 'XRP/GBP'
    topic = [
        {'method': 'subscribe', 'params': {'channel':'ticker', 'symbol': [f"{tickers}"], 'event_trigger': 'bbo'}}
         ]
    root = tk.Tk()
    root.title(f"{tickers} OB Best Bids")
    root.attributes("-topmost",True)
    message_label = tk.Label(root, text = "", font =("Helvetica", 16))
    message_label.pack(pady = 20)
    threading.Thread(target = windows_screen_asyncio_loop, args = (topic, message_label), daemon = True).start()
    root.mainloop()