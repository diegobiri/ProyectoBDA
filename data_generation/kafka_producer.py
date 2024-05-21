import json
from kafka import KafkaProducer
from json import dumps

def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        return None

#Parsea un archivo de texto con información de reservas y retorna una lista de diccionarios con los datos.
def parse_reservas(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.read().splitlines()
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
        return []

    reservas = []
    reserva = {}

    for line in lines:
        line = line.strip()
        if line.startswith("* Reserva"):
            if reserva:
                reservas.append(reserva)
                reserva = {}
            try:
                reserva_id_str = line.split("Reserva")[1].strip().split()[0].strip('*')
                reserva["Id_Reserva"] = int(reserva_id_str)
            except (IndexError, ValueError) as e:
                print(f"Error al extraer id_reserva de la línea: {line} - {e}")
                continue
        elif line:
            key, value = map(str.strip, line.split(":", 1))
            if key == "ID Cliente":
                reserva["ID Cliente"] = int(value)
            elif key == "Fecha Llegada":
                reserva["Fecha Llegada"] = value
            elif key == "Fecha Salida":
                reserva["Fecha Salida"] = value
            elif key == "Tipo Habitacion":
                reserva["Tipo Habitacion"] = value
            elif key == "Preferencias Comida":
                reserva["Preferencias Comida"] = value
            elif key == "Id Habitacion":
                reserva["Id Habitacion"] = int(value)
            elif key == "ID Restaurante":
                reserva["ID Restaurante"] = int(value)

    if reserva:
        reservas.append(reserva)

    return reservas

def convertir_a_json(reservas):
    return json.dumps(reservas, indent=4)

file_path = 'ProyectoBDA/data_Prim_ord/text/reservas.txt'  # Path del archivo
reservas = parse_reservas(file_path)
if reservas:
    reservas_json = convertir_a_json(reservas)
    with open('reservas.json', 'w') as json_file:
        json_file.write(reservas_json)




producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

res = read_json_file('reservas.json')

for element in res:
    message = {
        "id_reserva": element['id_reserva'],
        "id_cliente": element['id_cliente'],
        "fecha_llegada": element['fecha_llegada'],  
        "fecha_salida": element['fecha_salida'],
        "tipo_habitacion": element['tipo_habitacion'],  
        "preferencias_comida": element['preferencias_comida'],
        "id_habitacion": element['id_habitacion'],
        "id_restaurante": element['id_restaurante']  
    }
    producer.send('reservas_stream', value=message) 







