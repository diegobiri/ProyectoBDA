from pymongo import MongoClient
import json

def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        return None
    
class MongoDBOperations:
    def __init__(self, database_name,port):
        self.client = MongoClient(f'mongodb://localhost:{port}/')
        self.db = self.client[database_name]
        
    def create_clientes(self, collection_name,data):
        self.collection = self.db[collection_name]
        result = self.collection.insert_one(data)
        return result
    
clientes=read_json_file("ProyectoBDA/data_Prim_ord/json/clientes.json")


mongo_operations = MongoDBOperations('Clientes',27017)

for data in clientes:
    mongo_operations.create_clientes("clientes",data)