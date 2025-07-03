Rabbit is able to: 

1. display live spot price of (one or multiple) chose assets on an adjustable screen(alwasy in-front display of all open windows).
   
   Aim: monitor live prices
   
2. kraken_x_fx_arb.py is doing triangel arbitragy - foreign exchange, and stable coins only. It will print out those three involved exchange tickers, and profit rates in terminal constantly.
   
   Method: websockets and asyncio coroutines, live time on Kraken exchange. 

3. dump.py is a bash script to make transaction execution, assets information enquiry easier.
   
    Aim: do not need to make transaction over the Kraken exchange any more but pc terminals for efficent and quicker execution.

