from enum import IntEnum
from datetime import datetime

class  Reason(IntEnum):
    PLACE=0
    TRADE=1
    CANCEL=2
    INITIAL=3

BID=1
ASK=0
def parseSide(side):
    return BID if side=="bid" else ASK
def getIntReason(reason):
    if reason == "place":
        reason = Reason.PLACE 
    elif reason == "cancel":
        reason = Reason.CANCEL
    elif reason == "initial":
        reason = Reason.INITIAL
    elif reason == "trade":
        reason = Reason.TRADE
    else:
        reason=-1
        assert("unkown reason");
    return reason
    
class Update:
    def __init__(self,side,price,remaining,delta,reason,timestamp,snapshotID,socketSequence,orderType="change"):
        self.orderType=orderType
        self.side=side
        self.price=price
        self.remaining=remaining
        self.delta=delta
        self.reason=reason
        self.timestamp=timestamp
        self.socketSequence=socketSequence
        self.snapshotID=snapshotID
    def isBid(self):
        return self.side==BID
    def isInitial(self):
        return self.reason == Reason.INITIAL

    def getDateTime(self):
        return datetime.utcfromtimestamp(self.timestamp/1000)
    def getTimeStamp(self):
        return self.timestamp
    
        

class OrderBookEntry: # OrderBookEntry is a slot in the list DS

        def __init__(self, price, remaining, next = None): # next is of type OrderBookEntry
                self.price = price
                self.remaining = remaining
                self.next = next

        def getPrice(self):
                return self.price

        def getSize(self):
                return self.remaining

        def getNext(self):
                return self.next # return type is a direct pointer to an OrderBookEntry obj

        def setPrice(self, price):
                self.price = price

        def setSize(self, remaining):
                self.remaining = remaining

        def setNext(self, next): # next is of type OrderBookEntry
                self.next = next # changes the direct pointer to point to a different OrderBookEntry obj

class Asks:

        def __init__(self, head): # head is of type OrderBookEntry
                self.head = head # direct pointer to an OrderBookEntry obj

        def getTopAsk(self):
                return self.head # return type is a direct pointer to an OrderBookEntry obj

        def getTopLevel(self):
                return self.head # return type is a direct pointer to an OrderBookEntry obj

class Bids:

        def __init__(self, head): # head is of type OrderBookEntry
                self.head = head # direct pointer to an OrderBookEntry obj

        def getTopBid(self):
                return self.head # return type is a direct pointer to an OrderBookEntry obj

        def getTopLevel(self):
                return self.head # return type is a direct pointer to an OrderBookEntry obj

class OrderBook: # 1 singly linked list for each side (bid and ask) of the order book, the head of the bid list is the bid entry with the highest price and the head of the ask list is the ask entry with the lowest price
# each OrderBookEntry obj in the linked list must have price > $0.00 / BTC and remaining > 0 BTC
        def __init__(self,snapshotID,startTime,symbolIndex,bids=None, asks=None): # bids is of type Bids, asks is of type Asks
                self.orderbook = {"bid": bids, "ask": asks}
                self.startTime=startTime
                self.symbolIndex=symbolIndex
                self.snapshotID=snapshotID

        def getBids(self):
                return self.orderbook["bid"] # return type is a direct pointer to a Bids obj

        def getAsks(self):
                return self.orderbook["ask"] # return type is a direct pointer to an Asks obj

        def getSide(self, side):
                return self.orderbook[side] # return type is a direct pointer to a Bids or Asks obj
        def setSide(self, side,value):
                self.orderbook[side]=value 

        def getTopLevel(self, side):
            if self.orderbook[side] :
                return self.orderbook[side].getTopLevel() # return type is a direct pointer to an OrderBookEntry obj
            else:
                return None

        def updateBidSide(self, update): # updates the bid side of the order book at the level specified by the given price with the remaining amount given by remaining
                price,remaining,delta=update.price,update.remaining,update.delta
                previous_level = None
                level = self.getTopLevel("bid") # top level in the bid side of the order book
                updated = False
                #TODO Reduce code
                if level == None:
                    newLevel = OrderBookEntry(price, remaining, level)
                    self.setSide("bid", Bids(newLevel))
                else:
                    while level != None:
                        levelPrice = level.getPrice()
                        if price == levelPrice:
                                assert abs(level.getSize() +delta -remaining)<10**-5
                                if remaining > 0:
                                        level.setSize(remaining)
                                else: # remaining == 0
                                        if previous_level == None:
                                                self.getSide("bid").head = level.getNext()
                                        else: # previous_level is an actual OrderBookEntry obj and level is not the top level
                                                previous_level.setNext(level.getNext())
                                        level.setNext(None)
                                        #level = None
                                updated = True
                                break
                        elif price > levelPrice:
                                assert delta == remaining
                                assert update.reason != Reason.CANCEL
                                assert remaining >0
                                newLevel = OrderBookEntry(price, remaining, level)
                                if previous_level == None:
                                        self.getSide("bid").head = newLevel
                                else:
                                        previous_level.setNext(newLevel)
                                updated = True
                                break
                        else: #if price < levelPrice:
                                previous_level = level
                                level = level.getNext()
                    if not updated:
                        assert delta == remaining
                        previous_level.setNext(OrderBookEntry(price, remaining)) # in all cases, check whether you need to change the head of the list (it's assumed that top level is never None)

        def updateAskSide(self, update): # updates the ask side of the order book at the level specified by the given price with the remaining amount given by remaining
                price,remaining,delta=update.price,update.remaining,update.delta
                previous_level = None
                level = self.getTopLevel("ask") # top level in the ask side of the order book
                updated = False
                if level == None:
                    newLevel = OrderBookEntry(price, remaining, level)
                    self.setSide("ask", Asks(newLevel))
                else:
                    while level != None:
                        levelPrice = level.getPrice()
                        if price == levelPrice:
                                assert abs(level.getSize() +delta -remaining)<10**-5
                                if remaining > 0:
                                        level.setSize(remaining)
                                else: # remaining == 0
                                        if previous_level == None:
                                                self.getSide("ask").head = level.getNext()
                                        else: # previous_level is an actual OrderBookEntry obj and level is not the top level
                                                previous_level.setNext(level.getNext())
                                        level.setNext(None)
                                        #level = None
                                updated = True
                                break
                        elif price < levelPrice:
                                assert delta == remaining
                                assert update.reason != Reason.CANCEL
                                assert remaining >0
                                newLevel = OrderBookEntry(price, remaining, level)
                                if previous_level == None:
                                        self.getSide("ask").head = newLevel
                                else:
                                        previous_level.setNext(newLevel)
                                updated = True
                                break
                        else: #if price > levelPrice:
                                previous_level = level
                                level = level.getNext()
                    if not updated:
                        assert delta == remaining
                        previous_level.setNext(OrderBookEntry(price, remaining)) # in all cases, check whether you need to change the head of the list (it's assumed that top level is never None)

        def update(self, update): # updates the side (bid or ask) of the order book at the level specified by the given price with the remaining amount given by remaining
                if update.isBid():
                        self.updateBidSide(update)
                else: # side == "ask"
                        self.updateAskSide(update)
                self.lastUpdate=update
                if self.getBids() and self.getAsks():
                    assert self.getBids().getTopBid().getPrice()<self.getAsks().getTopAsk().getPrice()
                if self.getBids() :
                    b = self.listify(self.getBids().getTopBid())
                    prev_b = float("inf")
                    for bid in b:
                        assert prev_b > bid[0]
                        assert bid[1] > 0
                        prev_b = bid[0]
                if self.getAsks() :
                    a = self.listify(self.getAsks().getTopAsk())
                    prev_a = float("-inf")
                    for ask in a:
                        assert prev_a < ask[0]
                        assert ask[1] > 0
                        prev_a = ask[0]
                
                
        def getLastUpdate(self):
            return self.lastUpdate
        def listify(self,head):
            S = []
            n = head
            while n != None:
                    S.append((n.price, n.remaining))
                    n = n.getNext()
            return S
