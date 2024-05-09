from json import dumps
from kafka import KafkaProducer
import json

#Leemos el archivo de texto
def read_text_file(filename):
    try:
        with open(filename, 'r') as file:
            content = file.read()
            print(f"Contents of '{filename}':\n{content}")
    except FileNotFoundError:
        print(f"File '{filename}' not found.")

reservas = "ProyectoBDA/data_Prim_ord/text/reservas.txt"
#Creamos un json de reservas para poder usarlo en el kafka
def create_json_file(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
        
        

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    message = {
        "timestamp": int(datetime.now().timestamp() * 1000),
        "store_id": random.randint(1, 100),
        "product_id": fake.uuid4(),  
        "quantity_sold": random.randint(1, 20),  
        "revenue": round(random.uniform(100.0, 1000.0), 2)  
    }
    producer.send('sales_stream', value=message)
    sleep(1)  