import psycopg2
import pandas as pd
from io import StringIO
import boto3

def download_from_s3(bucket, key, access_key, secret_access_key):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
    )
    s3 = session.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    return StringIO(response['Body'].read().decode('utf-8'))

def create_and_load_tables(connection):
    cursor_clientes = connection.cursor()
    cursor_platos = connection.cursor()
    cursor_menus = connection.cursor()
    cursor_reservas = connection.cursor()
    # CREAMOS LAS TABLAS
    cursor_clientes.execute("""
    CREATE TABLE IF NOT EXISTS clientes (
        id_cliente SERIAL PRIMARY KEY,
        nombre VARCHAR(100),
        direccion VARCHAR(255),
        preferencias_alimenticias VARCHAR(255),
    );
    """)
    
    cursor_platos.execute("""
    CREATE TABLE IF NOT EXISTS platos (
        platoID SERIAL PRIMARY KEY,
        nombre VARCHAR(100),
        ingredientes VARCGAR(100),
        alergenos VARCHAR(100),
    );
    """)
    
    cursor_menus.execute("""
    CREATE TABLE IF NOT EXISTS menus (
        id_menu SERIAL PRIMARY KEY,
        precio FLOAT,
        disponibilidad BOOLEAN,
        id_restaurante INTEGER,
    );
    """)
    
    cursor_reservas.execute("""
    CREATE TABLE IF NOT EXISTS reservas (
        id_reserva SERIAL PRIMARY KEY,
        id_cliente INTEGER,
        fecha_llegada VARCHAR(100),
        fecha_salida VARCHAR(100),
        tipo_habitacion VARCHAR(100),
        preferencias_comida VARCHAR(100),
        id_habitacion INTEGER,
        id_restaurante INTEGER,
    );
    """)
    connection.commit()

    # Carga para la tabla de Empleados
    data = download_from_s3('clientes', 'path-to/empleados.csv', 'primOrd', 'bdaPrimOrd')
    df = pd.read_csv(data)
    for index, row in df.iterrows():
        cursor_clientes.execute("INSERT INTO clientes (id_cliente, nombre, direccion, preferencias_alimenticias) VALUES (%s, %s, %s, %s)",
                       (row['id_cliente'], row['nombre'], row['direccion'], row['preferencias_alimenticias']))
    connection.commit()
    cursor_clientes.close()
    
    # carga para la tabla de Menus
    data_platos = download_from_s3('menus', 'path-to/platos.csv', 'primOrd', 'bdaPrimOrd')
    df = pd.read_csv(data_platos)
    for index, row in df.iterrows():
        cursor_platos.execute("INSERT INTO platos (id_plato, nombre, ingredientes, alergenos) VALUES (%s, %s, %s, %s)",
                       (row['id_plato'], row['nombre'], row['ingredientes'], row['alergenos']))
    connection.commit()
    cursor_platos.close()
    
    # carga para la tabla de Menus
    data_menus = download_from_s3('menus', 'path-to/menus.csv', 'primOrd', 'bdaPrimOrd')
    df = pd.read_csv(data_menus)
    for index, row in df.iterrows():
        cursor_menus.execute("INSERT INTO menus (id_menu, nombre_plato, precio, id_restaurante) VALUES (%s, %s, %s, %s)",
                       (row['id_menu'], row['nombre_plato'], row['precio'],row['id_restaurante']))
    connection.commit()
    cursor_menus.close()
    
    # carga para la tabla de Hoteles
    data_reservas = download_from_s3('reservas', 'path-to/reservas.txt', 'primOrd', 'bdaPrimOrd')
    df = pd.read_csv(data_reservas)
    for index, row in df.iterrows():
        cursor_reservas.execute("INSERT INTO reservas (id_reserva,id_cliente,fecha_llegada,fecha_salida,tipo_habitacion,preferencias_comida,id_habitacion,id_restaurante) VALUES (%s, %s, %s,%s,%s,%s,%s,%s)",
                       (row['id_reserva'], row['id_cliente'], row['fecha_llegada'], row['fecha_salida'], row['tipo_habitacion'],row['preferencias_alimenticias'],row['id_habitacion'], row['id_restaurante']))
    connection.commit()
    cursor_reservas.close()

# Conexión a PostgreSQL
connection = psycopg2.connect(
    host="localhost",
    port="9999",
    database="PrimOrd",
    user="primOrd",
    password="bdaPrimOrd"
)
create_and_load_tables(connection)
connection.close()
