from neo4j import GraphDatabase
from kafka import KafkaProducer
import json
from json import dumps

def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        return None

# Detalles de conexión a Neo4j
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))  # Sustituir 'password' por tu contraseña de Neo4j

# Función para obtener datos de NEO4J y relaciones
def fetch_menus_platos_relaciones():
    query = """
    MATCH (m:Menus)-[r:ESTA]->(p:Platos)
    RETURN m AS menu, p AS plato, r
    """
    with neo4j_driver.session() as session:
        result = session.run(query)
        combined_data = []
        for record in result:
            menu = record['menu']
            plato = record['plato']
            combined_data.append({
                "id_menu": menu['id_menu'],
                "precio": menu['precio'],
                "disponibilidad": menu['disponibilidad'],
                "id_restaurante": menu['id_restaurante'],
                "id_plato": plato['platoId'],
                "nombre": plato['nombre'],
                "ingredientes": plato['ingredientes'],
                "alergenos": plato['alergenos']
            })
        return combined_data

# Obtener datos combinados
combined_data = fetch_menus_platos_relaciones()

# Escribir el archivo JSON resultante
with open('ProyectoBDA/data_Prim_ord/json/combined_data.json', 'w', encoding='utf-8') as file:
    json.dump(combined_data, file, ensure_ascii=False, indent=4)


combine = read_json_file('ProyectoBDA/data_Prim_ord/json/combined_data.json')

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

for element in combine:
    message = {
        "id_menu": element['id_menu'],
        "precio": element['precio'],
        "disponibilidad": element['disponibilidad'],  
        "id_restaurante": element['id_restaurante'],
        "id_plato": element['id_plato'],
        "nombre": element['nombre'],
        "ingredientes": element['ingredientes'],
        "alergenos": element['alergenos']
    }
    producer.send('menus_stream', value=message) 

