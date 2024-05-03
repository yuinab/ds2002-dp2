import os
from pymongo import MongoClient
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password='20oRGqjmo88JOF0k', connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.ncd6fc
# specify a collection
collection = db.dp2
path = "data"

for filename in os.listdir(path):
  with open(os.path.join(path, filename)) as f:
    try:
        file_data = json.load(f)
    except Exception as e:
        print(e, "error when loading", f)
    if isinstance(file_data, list):
        try:
            collection.insert_many(file_data)  
        except Exception as e:
            print(e, "when importing into Mongo")
    else:
        try:
            collection.insert_one(file_data)
        except Exception as e:
            print(e)
