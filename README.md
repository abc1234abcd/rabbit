**Rabbit is able to:** 

1. display live spot price of (one or multiple) chose assets on an adjustable screen(alwasy in-front display of all open windows).
   
   Aim: monitor live prices
   
2. kraken_x_fx_arb.py is doing triangel arbitragy - foreign exchange, and stable coins only. It will print out those three involved exchange tickers, and profit rates in terminal constantly.
   
   Method: websockets and asyncio coroutines, live time on Kraken exchange. 

3. dump.py is a bash script to make transaction execution, assets information enquiry easier.
   
    Aim: do not need to make transaction over the Kraken exchange any more but pc terminals for efficent and quicker execution.

4. rabbit_bot.py is an auto execution trading bot on kraken exchange which is doing arbitragy based on relative strength index(RSI).
   
   Aim: I use RSI as an indicator to generate buy and sell signal on xrp/usd market at live time. So far, it generates positive profit before transaction fee so I really didnot run it.    
   It connected to the telegram botfather too, so I will be getting live notification about the bot loggings through telegram messages.
   
5. kraken_market_info.py persisted kraken exchange data over different markets in MongoDB database. It should be serialized into the db but not indeed in this script.
