import sys
import pandas as pd
import pymongo
import json
import os


def import_content(filepath):
    mng_client = pymongo.MongoClient('localhost', 27017)
    mng_db = mng_client['BDA']
    collection_name = 'pullreq'
    db_cm = mng_db[collection_name]
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)

    data = pd.read_csv(file_res)
    print(type(data['date'][0]))
    data['date'] = pd.to_datetime(data['date']).dt.date.astype(str)
    print(data['date'])
    print(type(data['date'][0]))
    data_json = json.loads(data.to_json(orient='records'))
    db_cm.remove()
    db_cm.insert(data_json)

if __name__ == "__main__":
  filepath = './pullreq_events.csv'
  import_content(filepath)