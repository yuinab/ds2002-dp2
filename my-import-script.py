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
complete_imported = 0
complete_non_imported = 0
corrupted = 0


for (root, dirs, files) in os.walk(path):
    for f in files:
        file_path = os.path.join(root, f)
        try:
            with open(file_path, 'r') as file:
                file_data = json.load(file)
                if isinstance(file_data, list):
                    try:
                        complete_imported += len(file_data)
                        collection.insert_many(file_data)
                        
                    except Exception as e:
                        complete_non_imported += 1
                        print(e, "when importing into Mongo")
                else:
                    try:
                        complete_imported += 1
                        collection.insert_one(file_data)
                    except Exception as e:
                        complete_non_imported += 1
                        print(e)
        except Exception as e:
            print(e, "error when loading", f)
            corrupted += 1
print("complete imported", complete_imported)
print("complete non imported:", complete_non_imported)
print("corrupted:", corrupted)
