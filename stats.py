from database_connection import Database
db=Database()

def getContinuousRegionInfo(self):
    print("start time,endtime,delta(hrs), count")
    for startTime,endTime,count in db.getSnapshotsInfo():
        print(startTime,endTime,(endTime-startTime)/3600/1000,count)

