import pymongo
from kafka import KafkaProducer
from json import dumps


mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["Clientes"]  # Replace 'mydatabase' with your MongoDB database name
mongo_clientes_collection = mongo_db["clientes"]

# Function to fetch data from MongoDB
def fetch_mongo_clients():
    return list(mongo_clientes_collection.find({}, {'id_cliente': 1, 'nombre': 1, 'direccion': 1, 'preferencias_alimenticias': 1}))

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

clientes = fetch_mongo_clients()

for element in clientes:
    message = {
        "id_cliente": element['id_cliente'],
        "nombre": element['nombre'],
        "direccion": element['direccion'],  
        "preferencias_alimenticias": element['preferencias_alimenticias']    
    }
    producer.send('clientes_stream', value=message) 