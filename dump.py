'''
Kraken: restAPI.

reference: https://docs.kraken.com/api/docs/rest-api/get-tradable-asset-pairs

orderType: 
[market, limit, iceberg, stop-loss, take-profit, stop-loss-limit, take-profit-limit, trailing-stop, trailing-stop-limit, settle-position]

type:
[buy, sell]

pair:
[id or altname]

addOrderBatch: min of 2 and max 15.

cl_ord_id: client order identifier. it providers a unique identifier for each open order. 

'''
import requests, os, logging, datetime, time, json, urllib.parse, hashlib, hmac, base64
from pathlib import Path
from dotenv import load_dotenv

    
os.chdir(Path(__file__).parent.absolute())
logging.basicConfig(filename = f'krakenRestCli_{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log', format = '%(astime)s %(levelname) - 8s %(message)s', level = logging.INFO, datefmt ='%Y-%m-%d %H:%M:%S')

load_dotenv()
krakenapikey = os.getenv('KRAKENAPIKEY')
krakenapisecret = os.getenv('KRAKENAPISECRET')

#--------------
#---restAPI----
#--------------

def getServerTime():
    try:    
        serverTimeResp = requests.get('https://api.kraken.com/0/public/Time')
        if serverTimeResp.status_code != 200:
            raise requests.HPPTError(f"HTTP Error: {serverTimeResp.status_code}.")
        serverTime = serverTimeResp.json()
        unixTime = serverTime['result']['unixtime']
        serverDatetime = serverTime['result']['rfc1123']        
        return unixTime, serverDatetime
    except requests.RequestException as e:
        logging.error(f"Get Kraken server time failed {e}.")        

def getAssetInfo(ticker=None):
    try:
        url = 'https://api.kraken.com/0/public/Assets'        
        params = {}
        if ticker:
            params = {'asset': [ticker]}
        assetInfoResp = requests.get(url = url, params = params)
        if assetInfoResp.status_code != 200:
            raise requests.HTTPError(f"HTTP Error: {assetInfoResp.status_code}.")
        assetInfo = assetInfoResp.json()            
        return assetInfo
    except requests.RequestException as e:
        logging.error(f"Get Kraken asset information failed {e}.")
    
def getTradablePair(tickerA, tickerB):
    try:
        params = {'pair': tickerA +'/'+ tickerB}
        tradablePairResp = requests.get('https://api.kraken.com/0/public/AssetPairs', params = params)        
        if tradablePairResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken tradable pair failed {tradablePairResp.status_code}.")
        tradablePair = tradablePairResp.json()['result'][params['pair']]        
        return tradablePair
    except requests.RequestException as e:
        logging.error(f"Get Kraken tradable pair information failed {e}.")

def getTickerInfo(tickerA, tickerB):
    try:
        params = {'pair': tickerA + tickerB}
        tickerInfoResp = requests.get('https://api.kraken.com/0/public/Ticker', params = params)
        if tickerInfoResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken ticker information failed {tickerInfoResp.status_code}.")
        tickerInfo = tickerInfoResp.json()['result'][params['pair']]        
        return tickerInfo
    except requests.RequestException as e:
        logging.error(f"Get Kraken ticker infromation failed {e}")

def getohlc(tickerA, tickerB, interval, since):
    try:
        params ={'pair': tickerA + tickerB, 'interval': interval, 'since': since}
        ohlcResp = requests.get('https://api.kraken.com/0/public/OHLC', params = params)
        if ohlcResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken OHLC failed {ohlcResp.status_code}.")
        ohlc = ohlcResp.json()['result']        
        return ohlc
    except requests.RequestException as e:
        logging.error(f"Get Kraken OHLC failed {e}.")

def getOrderBook(tickerA, tickerB):
    try:
        params ={'pair': tickerA + tickerB, 'count': 500}
        orderBookResp = requests.get('https://api.kraken.com/0/public/Depth', params = params)
        if orderBookResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken order book failed {orderBookResp.status_code}.")
        orderBook = orderBookResp.json()['result']        
        return orderBook
    except requests.RequestException as e:
        logging.error(f"Get Kraken order book failed {e}.")

def getRecentTrades(tickerA, tickerB, minBefore):
    try:
        since = int(time.time()*1000) - minBefore*60*1000
        params = {'pair': tickerA + tickerB, 'since': since, 'count': 1000}
        recentTradesResp = requests.get("https://api.kraken.com/0/public/Trades", params = params)
        if recentTradesResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken recent trades failed {recentTradesResp.status_code}.")
        recentTrades = recentTradesResp.json()['result']             
        return recentTrades
    except requests.RequestException as e:
        logging.error(f"Get Kraken recent trades failed {e}.")


def getRecentSpread(tickerA, tickerB, minBefore):
    try:
        since = int(time.time()*1000) - minBefore*60*1000
        params ={'pair': tickerA + tickerB, 'since': since}
        recentSpreadResp = requests.get('https://api.kraken.com/0/public/Spread', params = params)
        if recentSpreadResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken recent spread failed {recentSpreadResp.status_code}")
        recentSpread = recentSpreadResp.json()['result']        
        return recentSpread
    except requests.RequestException as e:
        logging.error(f"Get Kraken recent spread failed {e}.")

#Kraken private channel rest API.
def krakenApiSign(urlpath, data, secret):    
    if isinstance(data, str):
        encoded = (str(json.loads(data)["nonce"]) + data).encode()
    else:
        encoded = (str(data["nonce"]) + urllib.parse.urlencode(data)).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()

def getAccBalance():    
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        krakenapisecret = os.getenv('KRAKENAPISECRET')
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
        accountBalance = accountBalanceResp.json()['result']        
        return accountBalance
    except requests.RequestException as e:
        logging.error(f"Get Kraken account balance failed {e}.")
    
def getExtendedBalance():
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        krakenapisecret = os.getenv('KRAKENAPISECRET')
        data = {'nonce': str(int(time.time()*1000))}
        urlpath = '/0/private/BalanceEx'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers ={
            'API-Key': krakenapikey,
            'API-Sign': apisignature,
        }
        extendedBalanceResp = requests.post(url, headers = headers, data = data)
        if extendedBalanceResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken extended balance failed {extendedBalanceResp.status_code}.")
        extendedBalance = extendedBalanceResp.json()['result']
        return extendedBalance
    except requests.RequestException as e:
        logging.error(f"Get Kraken extended balance failed {e}.")
        
def getTradeBalance():
    try:        
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        krakenapisecret = os.getenv('KRAKENAPISECRET')
        data = {'nonce': str(int(time.time()*1000)), 'asset': 'GBP'}
        urlpath = '/0/private/TradeBalance'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)        
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,
        }
        tradeBalanceResp = requests.post(url, headers = headers, data = data)
        if tradeBalanceResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken trade balance failed (tradeBalanceResp.status_code).")
        tradeBalance = tradeBalanceResp.json()['result']        
        return tradeBalance
    except requests.RequestException as e:
        logging.error(f"Get Kraken trade balance failed {e}.")

def getOpenOrders():
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        krakenapisecret = os.getenv('KRAKENAPISECRET')
        data ={'nonce': str(int(time.time()*1000))}
        urlpath = '/0/private/OpenOrders'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,
        }       
        openOrdersResp = requests.post(url, headers = headers, data = data)
        if openOrdersResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken open order failed {openOrdersResp.status_code}")
        openOrders = openOrdersResp.json()['result']        
        return openOrders
    except requests.RequestException as e:
        logging.error(f"Get Kraken open order failed {e}.")
        
def getClosedOrders():
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        krakenapisecret = os.getenv('KRAKENAPISECRET')
        data = {'nonce': str(int(time.time()*1000))}
        urlpath = '/0/private/ClosedOrders'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,
        }
        closedOrdersResp = requests.post(url, headers = headers, data =data)
        if closedOrdersResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken closed orders failed {closedOrdersResp.status_code}.")
        closedOrders = closedOrdersResp.json()['result']        
        return closedOrders
    except requests.RequestException as e:
        logging.error(f"Get Kraken closed orders failed {e}.")

def querryOrderInfo(txid):
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        krakenapisecret = os.getenv('KRAKENAPISECRET')
        data ={'nonce': str(int(time.time()*1000)), 'txid': txid }
        urlpath = '/0/private/QueryOrders'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,
        }
        orderInfoResp = requests.post(url, headers = headers, data =data)
        if orderInfoResp.status_code != 200:
            raise requests.HTTPError(f"Querry Kraken order information failed {orderInfoResp.status_code}.")
        orderInfo = orderInfoResp.json()
        return orderInfo
    except requests.RequestException as e:
        logging.error(f"Querry Kraken order information failed {e}.")

def getTradeHist():
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        krakenapisecret = os.getenv('KRAKENAPISECRET')
        data ={'nonce': str(int(time.time()*1000))}
        urlpath = '/0/private/TradesHistory'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,
        }
        tradeHistResp = requests.post(url, headers = headers, data =data)
        if tradeHistResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken trade History failed {tradeHistResp.status_code}.")
        tradeHist = tradeHistResp.json()
        return tradeHist
    except requests.RequestException as e:
        logging.error(f"Get Kraken trade HIstory failed {e}.")

def querryTradesInfo(txid, trades = False):
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        krakenapisecret = os.getenv('KRAKENAPISECRET')
        data ={'nonce': str(int(time.time()*1000)), 'txid': txid}
        if trades:
            data['trades'] = trades
        urlpath = '/0/private/QueryTrades'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,
        }
        if trades:
            data['trades'] = trades
        tradesInfoResp = requests.post(url, headers = headers, data =data)
        if tradesInfoResp.status_code != 200:
            raise requests.HTTPError(f"Querry Kraken trade inforamtion failed {tradesInfoResp.status_code}.")
        tradesInfo = tradesInfoResp.json()['result']
        return tradesInfo
    except requests.RequestException as e:
        logging.error(f"Querry Kraken trade information failed {e}.")

def getOpenPositions(txid=None, docalcs=False, consolidation= None):
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        krakenapisecret = os.getenv('KRAKENAPISECRET')
        data ={'nonce': str(int(time.time()*1000))}
        if txid is not None:
            data['txid'] = txid
        if docalcs:
            data['docalcs'] = docalcs
        if consolidation is not None:
            data['consolidation'] = consolidation
        urlpath = '/0/private/OpenPositions'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,
        }
        openPositionsResp = requests.post(url, headers = headers, data =data)
        if openPositionsResp.status_code != 200:
            raise requests.HTTPError(f"Get Kraken open postions failed {openPositionsResp.status_code}.")
        openPositions = openPositionsResp.json()['result']
        return openPositions
    except requests.RequestException as e:
        logging.error(f"Get Kraken open positions failed {e}.")

def addOrder(orderType, type, volume,  pair, krakenapisecret, userref = None, cl_ord_id = None,  displayvol=None, price = None, price2=None, trigger =None, leverage = None, reduce_only = None, stptype=None, oflags=None, timeinforce =None, starttm =None, expiretm = None, close = None,deadline = None, validate = False):
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')        
        data = {
            'nonce': str(time.time()*1000),
            'ordertype': orderType, 
            'type': type,
            'volume': volume,
            'pair': pair,
        }
        if userref is not None:
            data['userref'] = userref
        if cl_ord_id is not None:
            data['cl_ord_id'] = cl_ord_id
        if displayvol is not None:
            data['displayvol'] = displayvol
        if price is not None:
            data['price'] = price
        if price2 is not None:
            data['price2'] = price2
        if trigger is not None:
            data['trigger'] = trigger
        if leverage is not None:
            data['leverage'] = leverage
        if reduce_only is not None:
            data['reduce_only'] = reduce_only
        if stptype is not None:
            data['stptype'] = stptype
        if oflags is not None:
            data['oflags'] = oflags
        if timeinforce is not None:
            data['timeinforce'] = timeinforce
        if starttm is not None:
            data['starttm'] = starttm
        if expiretm is not None:
            data['expiretm'] = expiretm   
        if close is not None and isinstance(close, dict):
            data['close'] = close #close should be passed as an dictionary
        if deadline is not None:
            data['deadline'] = deadline
        if validate:
            data['validate'] = validate            
        urlpath = '/0/private/AddOrder'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature, 
        }
        addOrderResp = requests.post(url, headers = headers, data = data)
        if addOrderResp.status_code != 200:
            raise requests.HTTPError(f"Add order on Kraken failed {addOrderResp.status_code}.")
        addOrder = addOrderResp.json()
        return addOrder
    except requests.RequestException as e:
        logging.error(f"Add order on Kraken failed {e}.")
        
def addOrderBatch(orders, pair, krakenapisecret, deadline = None, validate=False):
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')       
        data ={
            'nonce': str(int(time.time()*1000)),
            'orders':[orders],
            'pair': pair,
        }
        if deadline is not None:
            data['deadline'] = deadline
        if validate:
            data['validate'] = validate
        urlpath = '/0/private/AddOrderBatch'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,           
        }
        addOrderBatchResp = requests.post(url, headers = headers, data = data)
        if addOrderBatchResp.status_code != 200:
            raise requests.HTTPError(f"Add order batch to Kraken failed {addOrderBatchResp.status_code}.")
        addOrderBatch = addOrderBatchResp.json()
    except requests.RequestException as e:
        logging.error(f"Add order batch to Kraken failed {e}.")

def amendOrder(krakenapisecret, txid=None, cl_ord_id=None, order_qty=None, display_qty=None, limit_price=None, trigger_price=None, post_only=False, deadline= None):
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        data ={
            'nonce': str(int(time.time()*1000)),
        }
        if txid is not None:
            data['txid'] = txid
        if cl_ord_id is not None:
            data['cl_ord_id'] = cl_ord_id
        if order_qty is not None:
            data['order_qty'] = order_qty
        if display_qty is not None:
            data['display_qty'] = display_qty
        if limit_price is not None:
            data['limit_price'] = limit_price
        if trigger_price is not None:
            data['trigger_price'] = trigger_price
        if post_only:
            data['post_only'] = post_only
        if deadline is not None:
            data['deadline'] = deadline
        urlpath ='/0/private/AmendOrder'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,           
        }
        amendOrderResp = requests.post(url, headers = headers, data = data)
        if amendOrderResp.status_code != 200:
            raise requests.HTTPError(f"Amend Kraken order failed {amendOrderResp.status_code}.")
        amendOrder = amendOrderResp.json()
        return amendOrder
    except requests.RequestError as e:
        logging.error(f"Amend Kraken order failed {e}.")

def cancelOrder(txid, cl_ord_id = None):
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        krakenapisecret = os.getenv('KRAKENAPISECRET')
        data = {
            'nonce': str(int(time.time()*1000)),
            'txid': txid,
        }
        if cl_ord_id is not None:
            data['cl_ord_id'] = cl_ord_id
        urlpath = '/0/private/CancelOrder'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,           
        }
        cancelOrderResp = requests.post(url, headers = headers, data = data)
        if cancelOrderResp.status_code != 200:
            raise requests.HTTPError(f"Cancel Kraken order failed {cancelOrderResp.status_code}.")
        cancelOrder = cancelOrderResp.json()
        return cancelOrder
    except requests.RequestError as e:
        logging.error(f"Cancel Kraken order failed {e}.")

def cancelAllOrder(krakenapisecret):
    try:
        load_dotenv()
        krakenapikey = os.getenv('KRAKENAPIKEY')
        data = {'nonce': str(int(time.time()*1000))}
        urlpath = '/0/private/CancelAll'
        url = 'https://api.kraken.com' + urlpath
        apisignature = krakenApiSign(urlpath, data, krakenapisecret)
        headers = {
            'API-Key': krakenapikey,
            'API-Sign': apisignature,           
        }
        cancelAllOrderResp = requests.post(url, headers = headers, data = data)
        if cancelAllOrderResp.status_code != 200:
            raise requests.HTTPError(f"Cancel Kraken all order failed {cancelAllOrderResp.status_code}.")
        cancelAllOrder = cancelAllOrderResp.json()
        return cancelAllOrder
    except requests.RequestError as e:
        logging.error(f"Cancel Kraken all order failed {e}.")

if __name__=='__main__':
    tradeHist = getTradeHist()
    print(tradeHist)
