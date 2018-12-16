
from database_connection import Database,Symbols
db=Database()
A=0
B=0
for x in db.getSnapshotsInfo(-1,A,B):
    print(x)
