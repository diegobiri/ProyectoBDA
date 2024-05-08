from neo4j import GraphDatabase
import csv
import json

def read_csv_file(filename):
    data = []
    with open(filename, 'r', encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header
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
        query = f"CREATE (n:{label} $properties) RETURN n"
        result = tx.run(query, properties=properties)
        return result
    
    def create_relationship(self, labelOrigin, idOrigin, labelEnd, idEnd, relationshipName):
        with self._driver.session() as session:
            result = session.write_transaction(self._create_relationship, labelOrigin, idOrigin, labelEnd, idEnd, relationshipName)
            return result

    @staticmethod
    def _create_relationship(tx, labelOrigin, idOrigin, labelEnd, idEnd, relationshipName):
        query = (
            f"MATCH (a:{labelOrigin} {{id: $idOrigin}}), (b:{labelEnd} {{id: $idEnd}}) "
            f"CREATE (a)-[r:{relationshipName}]->(b) "
            f"RETURN a, b, r"
        )
        parameters = {'idOrigin': idOrigin, 'idEnd': idEnd}
        result = tx.run(query, parameters)
        return result
    


uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"

neo4j_crud = Neo4jCRUD(uri, user, password)

readerMenus = read_csv_file("ProyectoBDA/data_Prim_ord/csv/menu.csv")
readerPlatos = read_csv_file("ProyectoBDA/data_Prim_ord/csv/platos.csv")
readerRelaciones = read_json_file("ProyectoBDA/data_Prim_ord/json/relaciones.json")

for element in readerMenus:
    neo4j_crud.create_node("Menus", {"id_menu": element[0], "precio": element[1], "disponibilidad": element[2], "id_restaurante": element[3]})

for element in readerPlatos:
    neo4j_crud.create_node("Platos", {"platoId": element[0], "nombre": element[1], "ingredientes": element[2], "alergenos": element[3]})

for element in readerRelaciones:
    neo4j_crud.create_relationship("Menus", element['id_menu'], "Platos", element['id_plato'], "ESTA")


neo4j_crud.close()


