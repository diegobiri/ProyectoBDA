from neo4j import GraphDatabase
import json


# Neo4j connection details
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))  # Replace 'your_password' with your Neo4j password


# Obtenemos los datos de NEO4J
def fetch_neo4j_menus():
 with neo4j_driver.session() as session:
        result = session.run("MATCH (n:Menus) RETURN n")
        return list(result)

def fetch_neo4j_platos():
 with neo4j_driver.session() as session:
        result = session.run("MATCH (n:Platos) RETURN n")
        return list(result)


def fetch_neo4j_relaciones():
 with neo4j_driver.session() as session:
        result = session.run("MATCH p=()-[r:ESTA]->() RETURN p")
        return list(result)


menus = fetch_neo4j_menus()
platos = fetch_neo4j_platos()
relaciones = fetch_neo4j_relaciones()

if isinstance(relaciones, list):
    combined_data = relaciones + menus + platos
else:
    combined_data = [relaciones] + menus + platos

# Escribir el archivo JSON resultante
with open('ProyectoBDA/data_Prim_ord/json/conbined.json', 'w', encoding='utf-8') as file:
    json.dump(combined_data, file, ensure_ascii=False, indent=4)

