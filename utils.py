from collections import deque, defaultdict, OrderedDict
from dataclasses import dataclass, field
from dateutil.parser import isoparse
from typing import List, Dict
@dataclass
class mt:
    ticker: str
    minutes: str = field(init=False)
    timestamp: str
    type: str
    side: str
    price: float
    qty: float
    volume: float
    def __post_init__(self):
        dt = isoparse(self.timestamp)
        self.minutes = dt.strftime('%Y-%m-%dT%H:%M')

@dataclass
class b_ob:
    ticker: str
    timestamp: str
    bids:[float]
    asks:[float]
    last_price: float
    day_low: float
    day_high: float
    day_pct: float
    day_volume: float
    day_vwap: float

@dataclass
class candle:
    ticker: str
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    no_trades: int
    volume: float
    vwap: float
    

    
class Exchange:
    def __init__(self):
        self.trades = []

    def newtrade(self,ticker, side, price, amount):
        nt = Trade(ticker,side,price,amount)
        self.trades.append(nt)
        return nt

    def freebal(self):
        self.freebalance = self.ccxt.fetch_balance()["free"]
        self.totalbalance = self.ccxt.fetch_balance()["total"]
        self.lastbalupdate = time.time()

class live_mt_deque:
    def __init__(self, ticker):
        self.ticker = ticker
        self.data = deque(maxlen=120)
    def __repr__(self):
        return f"{self.ticker}, {self.data}"

class live_candle_deque:
    def __init__(self, ticker):
        self.ticker = ticker
        self.data = deque(maxlen=1000)
    def __repr__(self):
        return f"{self.ticker}, {self.data}"