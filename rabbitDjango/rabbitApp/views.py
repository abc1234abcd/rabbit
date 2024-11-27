from django.http import HttpResponse
from datetime import datetime
import requests
import json, urllib, hmac, base64, hashlib, time, logging, os
from dotenv import load_dotenv
from pathlib import Path

os.chdir(Path(__file__).absolute().parent)
logging.basicConfig(filename = f'app_view_script_{datetime.now().strftime("%Y_%m_%d__%H_%M_%S")}.log', format = "%(asctime)s %(levelname)s-8s %(message)", level=logging.INFO, datefmt=("%Y_%m_%d_%H_%M_%S"))

load_dotenv()
krakenapikey = os.getenv('KRAKENAPIKEY')
krakenapisecret = os.getenv('KRAKENAPISECRET')

#exchange platform functions#
def krakenApiSign(urlpath, data, secret):    
    if isinstance(data, str):
        encoded = (str(json.loads(data)["nonce"]) + data).encode()
    else:
        encoded = (str(data["nonce"]) + urllib.parse.urlencode(data)).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()
def getAccBalance(krakenapikey, krakenapisecret):    
    try:
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

def homepage(reqeust):
    acc_balance = getAccBalance(krakenapikey, krakenapisecret)
    return HttpResponse("Hello to my Homepage")
