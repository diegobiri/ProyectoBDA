from neo4j import GraphDatabase
import csv
import json
def read_csv_file(filename):
    data =[]
    with open(filename, 'r',encoding="utf-8") as file:
        reader= csv.reader(file)
        for element in reader:
            data.append(element)        
    return data

def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        return None

class Neo4jCRUD:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None
        self._connect()

    def _connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def close(self):
        if self._driver is not None:
            self._driver.close()

    def create_node(self, label, properties):
        with self._driver.session() as session:
            result = session.write_transaction(self._create_node, label, properties)
            return result

    @staticmethod
    def _create_node(tx, label, properties):
        query = (
            f"CREATE (n:{label} $props) "
            "RETURN n"
        )
        result = tx.run(query, props=properties)
        
    @staticmethod
    def create_relationship(self,labelOrigin,labelEnd):
         with self._driver.session() as session:
            result = session.write_transaction(self._create_relationship, labelOrigin,labelEnd)
            return result
        
    @staticmethod
    def _create_relationship(tx, labelOrigin,labelEnd):
        query = (
            f"MATCH (n:{labelOrigin}),(c:{labelEnd}) "
            f"WHERE n.id='{labelOrigin}' and c.id='{labelEnd}' " 
        )
        result = tx.run(query)
        return result
    
uri = "bolt://localhost:7687"  
user = "neo4j"
password = "password"

neo4j_crud = Neo4jCRUD(uri, user, password)

readerMenus= read_csv_file("ProyectoBDA/data_Prim_ord/csv/menu.csv")
readerRelaciones=read_json_file("ProyectoBDA/data_Prim_ord/json/relaciones.json")
readerPlatos=read_csv_file("ProyectoBDA/data_Prim_ord/csv/platos.csv")

for element in readerMenus[1:]:
    node_properties = {
        "id_menu": element[0], 
        "precio": element[1],
        "disponibilidad":element[2],
        "id_restaurante":element[3]
        }
    neo4j_crud.create_node("Menus", node_properties)

for element in readerPlatos[1:]:
    node_properties = {
        "platoID": element[0], 
        "nombre": element[1],
        "ingredientes":element[2],
        "alergenos":element[3]
        }
    neo4j_crud.create_node("Platos", node_properties)

for element in readerRelaciones:
    neo4j_crud.create_relationship("id_menu",element[0], "id_plato",element[1])