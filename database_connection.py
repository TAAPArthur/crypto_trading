"""
Interfaces with the database
"""
import mysql.connector
from OrderBookDS import OrderBook,Update
from database_info import *
from enum import IntEnum

SYMBOLS=["btcusd","ethusd","zecusd","bchusd","ltcusd"]
class Symbols(IntEnum):
    BTCUSD=0
    ETHUSD=1
    ZECUSD=2
    BCHUSD=3
    LTCUSD=4

class Database:

    def __init__(self):
        self.conn = mysql.connector.connect(user=DATABASE_USERNAME,password=DATABASE_PASSWORD, database=DATABASE_NAME,host=DATABASE_HOST)
        self.cur = self.conn.cursor(prepared=True,raw=False)
        self.updateQuery = "INSERT INTO {} (Type,Side,Price,Remaining,Delta,Reason,TimeStamp,SocketSequence,SnapshotID) VALUES (0,%s,%s,%s,%s,%s,%s,%s,%s)".format(UPDATES_TABLE)
        self.getAllQuery = "SELECT * FROM {} ORDER BY TimeStamp ASC".format(UPDATES_TABLE)
        self.getInsertSnapshotQuery = "INSERT INTO {}(TimeStamp,SymbolIndex,GapTime,Reason,StartTime) VALUES (%s,%s,%s,%s,%s)".format(SNAPSHOT_TABLE)
        self.getUpdatesQuery="""
            SELECT Side+0,Price,Remaining,Delta,Reason,TimeStamp,SocketSequence,SnapshotID FROM {} INNER JOIN
                (SELECT ID from {}
                    WHERE SymbolIndex=%s AND
                        TimeStamp >= %s AND (TimeStamp < %s OR %s =0)
                    Order By TimeStamp ASC LIMIT 1
                ) AS T
            ON T.ID=SnapshotID
            ORDER BY SocketSequence ASC;""".format(UPDATES_TABLE,SNAPSHOT_TABLE)
        self.getUpdatesStatsQuery="""
            SELECT T.SymbolIndex, T.TimeStamp, MAX(U.TimeStamp), COUNT(*) FROM {} AS U
            INNER JOIN  {} AS T
            ON T.ID=SnapshotID
            WHERE (SymbolIndex=%s or %s=-1) AND
                T.TimeStamp >= %s AND (T.TimeStamp < %s OR %s =0)
            GROUP BY SnapshotID
            ORDER BY T.TimeStamp ASC;""".format(UPDATES_TABLE,SNAPSHOT_TABLE)
    
    def getUpdateTuple(self,update):
        return (update.side,update.price,update.remaining,update.delta,update.reason,update.timestamp,update.socketSequence,update.snapshotID)
    def addUpdate(self,update):
       self.cur.execute(self.updateQuery, self.getUpdateTuple(update))

    def addManyUpdates(self,updates):
       self.cur.executemany(self.updateQuery, updates)

    def createSnapshot(self,timestamp,symbolIndex,delta,reason,startTime,):
        self.cur.execute(self.getInsertSnapshotQuery,(timestamp,symbolIndex,delta,reason,startTime))
        orderbook = OrderBook(snapshotID=self.cur.lastrowid,startTime=timestamp,symbolIndex=symbolIndex)
        return orderbook
    
    def getUpdates(self,symbolIndex=0,startTime=0,endTime=0):
        self.cur.execute(self.getUpdatesQuery,(symbolIndex,startTime,endTime,endTime))
        for row in self.cur:
            row=list(map(lambda x:x if type(x)==int else float(x.decode()),row))
            yield Update(side=row[0],price=row[1],remaining=row[2],delta=row[3],reason=row[4],timestamp=row[5],socketSequence=row[6],snapshotID=row[7])

    """
    Returns a list of tuples for all the continous regions starting after startTime and ending before endTime
    The values of the tuples are startTime,endTime and number of updates
    Note that the number of updates includes the number of updates to populate the order book from scratch (ie the inital size of the orderbook)
    """
    def getSnapshotsInfo(self,symbolIndex=0,startTime=0,endTime=0):
        self.cur.execute(self.getUpdatesStatsQuery,(symbolIndex,symbolIndex,startTime,endTime,endTime))
        for row in self.cur:
            yield row

    """
    Returns an iterator of OrderBooks over the first continous region starting after starTime and ending before endTime
    Values of 0 are wild cards
    """
    def getSnapshots(self,symbolIndex=0,startTime=0,endTime=0):
        currentOrderbook=None
        snapshotID=0
        initial=True
        for update in self.getUpdates(symbolIndex,startTime,endTime):
            if update.isInitial() or update.timestamp < startTime:
                assert(snapshotID<=0 or update.snapshotID == snapshotID)
                if not currentOrderbook:
                    currentOrderbook = OrderBook(snapshotID=update.snapshotID,startTime=update.getTimeStamp(),symbolIndex=None)
                currentOrderbook.update(update)
                assert initial
            elif update.timestamp>endTime and endTime!=0:
                break
            else:
                initial=False
                currentOrderbook.update(update)
                yield currentOrderbook
    
    def commit(self):
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()
