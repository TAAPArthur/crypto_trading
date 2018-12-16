"""
This file connects to the database and logs data
"""
import websocket, json
import time
import traceback,sys,os
from OrderBookDS import OrderBookEntry,Update, Asks, Bids, OrderBook, getIntReason,parseSide
from database_connection import Database,SYMBOLS
from threading import Thread
from enum import IntEnum



# {"exchange": "gemini", "time": 154074.7235, "socket_sequence": 13, "orderbook_snapshot": OrderBook(bids, asks)}
# initial_orderbook, orderbook_snapshots, orderbook_updates

class  ReconnectReason(IntEnum):
    OTHER=0
    KILLED=1
    ERROR=2
    MISSED_MESSAGE=3


class State():
    START_TIME=0
    def __init__(self,symbolIndex,reconnectReason=ReconnectReason.KILLED):
        self.symbolIndex=symbolIndex
        self.reconnectReason=reconnectReason
        self.database = Database()
        self.lastMessageTime=None
        self.reset()
    def reset(self):
        self.previous_socket_sequence = -1 # exchange's first message on connect has socket_sequence 0
        self.orderbook = None
    def __del__(self):
        print("destroying state")
        self.database.close()

def get_update_from_event(event,timestamp,seqNum,snapshotID):
    side, price, size, delta, reason = event["side"], float(event["price"]), float(event["remaining"]), float(event["delta"]), event["reason"]
    update = Update(side=parseSide(side),price=price,remaining=size,delta=delta,reason=getIntReason(reason),timestamp=timestamp,socketSequence=seqNum,snapshotID=snapshotID)
    return update
    
def on_message(ws, message,state):
        frame = json.loads(message)
        socket_sequence = frame["socket_sequence"]
        print("SEQ:", socket_sequence,SYMBOLS[state.symbolIndex])
        # print("time:", time.time())
        try:
            # global orderbook_updates
            if socket_sequence - state.previous_socket_sequence == 1:
                state.previous_socket_sequence = socket_sequence
            else:
                print("restarting")
                state.reconnectReason=ReconnectReason.MISSED_MESSAGE
                state.database.commit()
                # raise NameError("There is a gap in the socket sequence.")
                print("Missed message; websocket closed")
                ws.close()
                return
            if frame["type"]=="heartbeat":
                return
            events = frame["events"]
            if socket_sequence == 0:
                if(not events):
                    return
                print("loading current order book:")
                timestamp = time.time()*1000
                delta=(timestamp-state.lastMessageTime*1000) if state.lastMessageTime else 0
                state.orderbook=state.database.createSnapshot(timestamp,state.symbolIndex,delta,state.reconnectReason,State.START_TIME)             
                i=0
                listOfUpdates=[]
                for event in events:
                    i+=1
                    update = get_update_from_event(event,timestamp,socket_sequence, state.orderbook.snapshotID)
                    state.orderbook.update(update)
                    listOfUpdates.append(state.database.getUpdateTuple(update))
                state.database.addManyUpdates(listOfUpdates)
                state.reconnectReason=ReconnectReason.OTHER
            else: # if socket_sequence >= 1
                assert len(events)<3
                timestamp = frame["timestampms"]
                assert(state.orderbook)
                event = events[-1]
                event_type = event["type"]
                if event_type == "change": # when an order is placed or canceled
                        update = get_update_from_event(event,timestamp,socket_sequence, state.orderbook.snapshotID)
                        state.orderbook.update(update)
                        state.database.addUpdate(update)
            state.database.commit()
            state.lastMessageTime=time.time()
        except:
            print("error")
            traceback.print_exc(file=sys.stdout)
            state.reconnectReason=ReconnectReason.ERROR
            ws.close()
            print("exiting")
            os._exit(1) 
def runWebSocket(symbolIndex):
    state=State(symbolIndex,ReconnectReason.KILLED)
    while 1:
        ws = websocket.WebSocketApp("wss://api.gemini.com/v1/marketdata/{}?heartbeat=true&bids=true&offers=true".format(SYMBOLS[symbolIndex]), on_message = lambda ws,message: on_message(ws,message,state))
        ws.run_forever()
        state.reset()
        print("websocket has been closed")

def start():
    State.START_TIME=int(time.time()*1000)
    for i in range(len(SYMBOLS)):
        Thread(target=lambda :runWebSocket(i)).start()

if __name__ == "__main__":
    start()


