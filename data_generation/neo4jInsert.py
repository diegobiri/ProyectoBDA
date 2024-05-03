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
        

    def atracciones_visitante(self, nombre):
        with self._driver.session() as session:
            return session.read_transaction(self._atracciones_visitante, nombre)
          
    @staticmethod
    def _atracciones_visitante(tx, nombre):
        query = (
            f"MATCH (n:Visitantes)-[]-(v:Atracciones) where n.visitor_name='{nombre}' RETURN v.attraction_name"
        )
        return tx.run(query)
    
    def create_relationship(self,labelOrigin,propertyOrigin,labelEnd,propertyEnd,relationshipName,fecha):
         with self._driver.session() as session:
            result = session.write_transaction(self._create_relationship, labelOrigin,propertyOrigin,labelEnd,propertyEnd,relationshipName,fecha)
            return result
        
    @staticmethod
    def _create_relationship(tx, labelOrigin,propertyOrigin,labelEnd,propertyEnd,relationshipName,fecha):
        query = (
            f"MATCH (n:{labelOrigin}),(c:{labelEnd}) "
            f"WHERE n.id='{propertyOrigin}' and c.id='{propertyEnd}' " 
            f"CREATE (n)-[:{relationshipName} {{fecha:'{fecha}'}} ]->(c)"
        )
        result = tx.run(query)
        return result
    
uri = "bolt://localhost:7687"  
user = "neo4j"
password = "password"

neo4j_crud = Neo4jCRUD(uri, user, password)

readerAtracciones= read_csv_file("Neo4j/Atracciones.csv")
readerInteracciones=read_csv_file("Neo4j/Interacciones.csv")
readerVisitantes=read_csv_file("Neo4j/Visitantes.csv")

for element in readerAtracciones[1:]:
    node_properties = {
        "id": element[0], 
        "attraction_name": element[1],
        "category":element[2],
        "location":element[3]
        }
    neo4j_crud.create_node("Atracciones", node_properties)

for element in readerVisitantes[1:]:
    node_properties = {
        "id": element[0], 
        "visitor_name": element[1],
        "age":element[2],
        "gender":element[3]
        }
    neo4j_crud.create_node("Visitantes", node_properties)

for element in readerInteracciones[1:]:
    neo4j_crud.create_relationship("Visitantes",element[0], "Atracciones",element[1],"VISITA", element[2] + element[3])