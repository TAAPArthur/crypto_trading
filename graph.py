#generate a graph of market data

import matplotlib.pyplot as plt
import sys

from OrderBookDS import Update,OrderBook
from database_connection import Database, SYMBOLS

symbol,startTime,endTime=sys.argv[1:]

symbolIndex=int(symbol) if symbol.isnumeric() else [i for i in range(len(SYMBOLS)) if SYMBOLS[i]==symbol][0]
startTime=int(startTime)
endTime=int(endTime)
database=Database()

while True:
    bids=[]
    asks=[]
    time=[]
    for snapshot in database.getSnapshots(symbolIndex,startTime,endTime):
        time.append(snapshot.getLastUpdate().getDateTime())
        #print(time[-1],snapshot.getBids().getTopBid().getPrice(),snapshot.getAsks().getTopAsk().getPrice())
        assert snapshot.getBids().getTopBid().getPrice()<snapshot.getAsks().getTopAsk().getPrice()
        bids.append(snapshot.getBids().getTopBid().getPrice())
        asks.append(snapshot.getAsks().getTopAsk().getPrice())
    if not time:
        break
    print(len(time))
    startTime=snapshot.startTime+1
    plt.plot(time,bids)
    plt.plot(time,asks)
    plt.ylabel("$")
    plt.xlabel("Time")
    plt.show()
        
