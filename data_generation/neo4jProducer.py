from neo4j import GraphDatabase
import json

# Detalles de conexión a Neo4j
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))  # Sustituir 'password' por tu contraseña de Neo4j

# Funciones para obtener datos de NEO4J
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
        result = session.run("MATCH ()-[r:ESTA]->() RETURN r")
        return [record["r"] for record in result]


combined_data = []
menus = fetch_neo4j_menus()
platos = fetch_neo4j_platos()
relaciones = fetch_neo4j_relaciones()

# Combinar datos según las relaciones
for menu in menus:
    for plato in platos:
        combined_data.append({
            "menu_item_id": menu[0]['id_menu'],
            "precio": menu[0]['precio'],
            "disponibilidad": menu[0]['disponibilidad'],
            "restaurante_item_id": menu[0]['id_restaurante'],
            "plato_item_id": plato[0]['id_plato'],
            "nombre": plato[0]['nombre'],
            "ingredientes": plato[0]['ingredientes'],
            "alergenos": plato[0]['alergenos']
        })
        break

# Escribir el archivo JSON resultante
with open('combined_data.json', 'w', encoding='utf-8') as file:
    json.dump(combined_data, file, ensure_ascii=False, indent=4)

