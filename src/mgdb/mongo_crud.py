from typing import Any
import os
import pandas as pd
import pymongo
import json
from ensure import ensure_annotations


from typing import Any
import os
import pandas as pd
from pymongo.mongo_client import MongoClient
import json
from ensure import ensure_annotations


class mongo_operation:
    
    def __init__(self,client_url: str, database_name: str):
        self.client_url = client_url
        self.database_name = database_name
        self.client = MongoClient(self.client_url)
    
    def create_database(self):
        self.database = self.client[self.database_name]
        return self.database
    
    def create_collection(self,collection_name):
        self.collection= self.create_database()[collection_name] 
        return self.collection
    
    def insert_record(self,record: dict, collection_name: str) -> Any:
        if type(record) == list:
            for data in record:
                if type(data) != dict:
                    raise TypeError("record must be in the dict")    
            collection=self.create_collection(collection_name)
            collection.insert_many(record)
        elif type(record)==dict:
            collection=self.create_collection(collection_name)
            collection.insert_one(record)
    
    def bulk_insert(self,datafile,collection_name:str=None):
        self.path=datafile
        
        if self.path.endswith('.csv'):
            pd.read.csv(self.path,encoding='utf-8')
            
        elif self.path.endswith(".xlsx"):
            dataframe=pd.read_excel(self.path,encoding='utf-8')
            
        datajson=json.loads(dataframe.to_json(orient='record'))
        collection=self.create_collection(collection_name)
        collection.insert_many(datajson)

# if __name__ == "__main__":
#     uri = "mongodb+srv://thnhan3011:30113011@cluster0.ejtjszb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
#     db_name = "TESTDB"
#     collection_name = "employee"

#     mgdb = mongo_operation(uri,db_name)
#     data = {
#         "name" : "Nguyen Thien Nhan",
#         "age" : 21,
#         "education" : "SPKT"
#     }
#     mgdb.insert_record(data, collection_name)
