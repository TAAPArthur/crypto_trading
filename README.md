# Example to get info of continous ranges of data for Bitcoin between time A and time B in milliseconds

```
from database_connection import Database,Symbols
db=Database()
A=0
B=0
for x in db.getSnapshotsInfo(Symbols.BTCUSD,A,B):
    print(x)
```

#Graphing
See graph.py
