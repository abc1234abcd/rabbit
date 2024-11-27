import asyncio
import json
from websockets import connect, exceptions
import pandas as pd
import time

class kraken_websocket_subscription:
    def __init__(self, queue_pub, topic_pub):
        self.socket = 'wss://ws.kraken.com/v2'        
        self.ws = None
        self.reconnect_delay = 0
        self.ping_interval = 30
        self.queue_pub = queue_pub
        self.topic_pub = topic_pub
             
    async def connect(self):
        while True:
            try:
                async with connect(self.socket) as websocket:
                    self.ws = websocket
                    await self.ws.send(json.dumps({"method": "ping"}))
                    await self.subscribe(self.topic_pub)                    
                    async for message in websocket:
                        await self.on_message(message)                        
            except exceptions.ConnectionClosedError as e:
                print(f"Websocket connection closed: {e}.Reconnecting in {self.reconnect_delay} seconds...")               
            except Exception as e:
                print(f"WebSocket error: {e}. Reconnecting in {self.reconnect_delay} seconds...")  

    async def subscribe(self,  topic_pub):
        self.topic_pub = self.topic_pub
        try:
            for element in self.topic_pub:            
                await self.ws.send(json.dumps(element))                
                async def send_30s_ping():
                    while True:
                        await asyncio.sleep(self.ping_interval)
                        try:                          
                            await self.ws.send(json.dumps({"method": "ping"}))
                        except Exception as e:
                            print(f"Error in sending 30s ping: {e}")
                            break
                asyncio.create_task(send_30s_ping())
        except Exception as e:
            print(f"Subscription error: {e}")

    async def on_message(self, message):        
        try:
            message = json.loads(message)
            #print(message)
            await self.queue_pub.put(message)                
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
        except Exception as e:
            print(f"Error in on_message: {e}")
    async def on_close(self):
        print("websocket is closed!")