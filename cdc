import pandas as pd
import os
from datetime import datetime
now = datetime.now()

class CDC:
    def __init__(self) -> None:
        self.newDF = pd.DataFrame
        self.OldSnap = pd.DataFrame
    def readFile(self,path):
        self.newDF = pd.read_csv(path)
    def readOldSS(self,snapShotPath):
        directories = [d for d in os.listdir(snapShotPath) if os.path.isdir(os.path.join(snapShotPath, d))]
        latest_folder = max(directories, key=lambda d: os.path.getctime(os.path.join(snapShotPath, d)))
        files = [d for d in os.listdir(snapShotPath+'/'+latest_folder) if os.path.isfile(os.path.join(snapShotPath+'/'+latest_folder, d))]
        self.OldSnap = pd.read_parquet(snapShotPath+latest_folder+'/'+files[0], engine='pyarrow')
    def writeFile(self):
        dt_string = now.strftime("%d%m%Y%H%M%S")
        snapshotPath = 'out/{}/'.format(dt_string)
        if not os.path.exists(snapshotPath):
            os.makedirs(snapshotPath)
        self.newDF.to_parquet(snapshotPath+'myfile.parquet')
    def applyCDC(self):
        df = self.OldSnap.append(self.newDF)\
           .drop_duplicates(subset=['name','id'], keep='last')\
           .sort_values('id').reset_index(drop=False)
        self.newDF = df


cdc = CDC()
cdc.readFile('data copy 2.csv')
cdc.readOldSS(snapShotPath=r'/Users/guna/FlutterDev/AppsFiles/text-analytics/out/')
cdc.writeFile()


#df.to_parquet('out/myfile.parquet')
