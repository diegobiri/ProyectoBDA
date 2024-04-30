import psycopg2
from psycopg2 import Error
import csv
def insert_data(data):
    connection=None
    try:
        # Establishing connection to PostgreSQL database
        connection = psycopg2.connect(
            user="primOrd",
            password="bdaPrimOrd",
            host="localhost",
            port="9999",
            database="PrimOrd"
        )

        # Creating a cursor object using the cursor() method
        cursor = connection.cursor()
        
        create_table_empleados = '''CREATE TABLE IF NOT EXISTS Empleados (
                                id_empleado INT AUTO_INCREMENT PRIMARY KEY,
                                nombre VARCHAR(255) NOT NULL,
                                posicion VARCHAR(255) NOT NULL,
                                fecha_contratacion VARCHAR(255) NOT NULL
                            );'''
        
        create_table_hoteles = '''CREATE TABLE IF NOT EXISTS Hoteles (
                                id_hotel INT AUTO_INCREMENT PRIMARY KEY,
                                nombre_hotel VARCHAR(255) NOT NULL,
                                direccion_hotel VARCHAR(255) NOT NULL
                            );'''

        # Executing the SQL command to create the table
        cursor.execute(create_table_empleados)
        cursor.execute(create_table_hoteles)
        connection.commit()
        print("Table created successfully")

        # Constructing the PostgreSQL INSERT statement
        insert_query_empleados = """ INSERT INTO Empleados (id_empleado, nombre, posicion,fecha_contratacion)
                           VALUES (%s, %s, %s, %s)"""

        insert_query_hoteles = """ INSERT INTO Hoteles (id_hotel, nombre_hotel, direccion_hotel)
                           VALUES (%s, %s, %s)"""

        # Executing the SQL command with the provided data
        cursor.execute(insert_query_empleados, data)
        cursor.execute(insert_query_hoteles, data)

        # Commit your changes to the database
        connection.commit()

        print("Data inserted successfully")

    except (Exception, Error) as error:
        print("Error while inserting data into PostgreSQL:", error)

    finally:
        # Closing database connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# Lectura del archivo csv 
def read_csv_file(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            print(row)


# Example data to be inserted into the database
file_empleados = read_csv_file("../data_Prim_ord/csv/empleados.csv")
file_hoteles = read_csv_file("../data_Prim_ord/csv/hoteles.csv")
# Call the insert_data function with the example data
insert_data(file_empleados)
insert_data(file_hoteles)