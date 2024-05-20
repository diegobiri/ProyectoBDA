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
    cursor_empleados = connection.cursor()
    cursor_habitaciones = connection.cursor()
    cursor_menus = connection.cursor()
    cursor_hoteles = connection.cursor()
    # CREAMOS LAS TABLAS
    cursor_empleados.execute("""
    CREATE TABLE IF NOT EXISTS empleados (
        id_empleado SERIAL PRIMARY KEY,
        nombre VARCHAR(100),
        posicion VARCHAR(255),
        fecha_contratacion VARCHAR(255),
    );
    """)
    
    cursor_habitaciones.execute("""
    CREATE TABLE IF NOT EXISTS habitaciones (
        numero_habitacion SERIAL PRIMARY KEY,
        categoria VARCHAR(100),
        tarifa_por_noche FLOAT
    );
    """)
    
    cursor_menus.execute("""
    CREATE TABLE IF NOT EXISTS menus (
        id_menu SERIAL PRIMARY KEY,
        nombre_plato VARCHAR(100),
        precio FLOAT
        id_restaurante INTEGER,
    );
    """)
    
    cursor_hoteles.execute("""
    CREATE TABLE IF NOT EXISTS hoteles (
        id_hotel SERIAL PRIMARY KEY,
        nombre_hotel VARCHAR(100),
        direccion_hotel VARCHAR(100),
    );
    """)
    connection.commit()

    # Carga para la tabla de Empleados
    data = download_from_s3('empleados-bucket', 'path-to/empleados.csv', 'primOrd', 'bdaPrimOrd')
    df = pd.read_csv(data)
    for index, row in df.iterrows():
        cursor_empleados.execute("INSERT INTO empleados (id_empleado, nombre, posicion, fecha_contratacion) VALUES (%s, %s, %s, %s)",
                       (row['id_empleado'], row['nombre'], row['posicion'], row['fecha_contratacion']))
    connection.commit()
    cursor_empleados.close()
    
    # carga para la tabla de Habitaciones
    data_habitaciones = download_from_s3('habitaciones-bucket', 'path-to/habitaciones.csv', 'primOrd', 'bdaPrimOrd')
    df = pd.read_csv(data_habitaciones)
    for index, row in df.iterrows():
        cursor_habitaciones.execute("INSERT INTO habitaciones (numero_habitacion, categoria, tarifa_por_noche) VALUES (%s, %s, %s)",
                       (row['numero_habitacion'], row['categoria'], row['tarifa_por_noche']))
    connection.commit()
    cursor_habitaciones.close()
    
    # carga para la tabla de Menus
    data_menus = download_from_s3('menus-bucket', 'path-to/menus.csv', 'primOrd', 'bdaPrimOrd')
    df = pd.read_csv(data_menus)
    for index, row in df.iterrows():
        cursor_menus.execute("INSERT INTO menus (id_menu, nombre_plato, precio, id_restaurante) VALUES (%s, %s, %s, %s)",
                       (row['id_menu'], row['nombre_plato'], row['precio'],row['id_restaurante']))
    connection.commit()
    cursor_menus.close()
    
    # carga para la tabla de Hoteles
    data_hoteles = download_from_s3('menus-bucket', 'path-to/hoteles.csv', 'primOrd', 'bdaPrimOrd')
    df = pd.read_csv(data_hoteles)
    for index, row in df.iterrows():
        cursor_hoteles.execute("INSERT INTO hoteles (id_hotel, nombre_hotel, direccion_hotel) VALUES (%s, %s, %s)",
                       (row['id_hotel'], row['nombre_hotel'], row['direccion_hotel']))
    connection.commit()
    cursor_hoteles.close()

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
