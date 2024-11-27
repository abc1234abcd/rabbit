import asyncio
from websockets import connect, exceptions
import json


class kraken_private_ws_subscribe:
    def __init__(self, queue_pri, topic_pri):
        self.topic_pri = topic_pri
        self.socket = "wss://ws-auth.kraken.com/v2"
        self.reconnect_delay = 0
        self.ping_interval = 30
        self.ws = None
        self.queue_pri = queue_pri
        
    async def connect(self):
        while True:
            try:
                async with connect(self.socket) as websocket_private:
                    self.ws = websocket_private
                    await self.ws.send(json.dumps({'method':'ping'}))
                    await self.subscribe(self.topic_pri)
                    async for message in websocket_private:
                        await self.on_message(message)
            except exceptions.ConnetionClosedError as e:
                print(f"websocket private connection closed: {e}. Reconnecting in {self.reconnect_delay} seconds...")
            except Exception as e:
                print(f"websocket private error: {e}. Reconnecting in {self.reconnect_delay} seconds...")
    async def subscribe(self, topic_pri):
        self.topic_pri = self.topic_pri
        try:
            for element in self.topic_pri:
                await self.ws.send(json.dumps(element))
                async def send_30s_ping():
                    while True:   
                        await asyncio.sleep(self.ping_interval)
                        try:
                            await self.ws.send(json.dumps({'method': 'ping'}))
                        except Exception as e:
                            print(f"error in sending 30s ping: {e}.")
                asyncio.create_task(send_30s_ping())
        except Exception as e:
            print(f"subscription error {e}.")
    async def on_message(self, message):
        try:
            message = json.loads(message)
            #print(message)
            await self.queue_pri.put(message)
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
        except Exception as e:
            print(f"error in on_message: {e}")
    async def on_close(self):
        print("websocket is closed!")
            
                        
                    
        
    