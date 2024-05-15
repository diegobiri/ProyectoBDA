from json import dumps
from kafka import KafkaProducer
import json

#Leemos el archivo de texto
def read_text_file(filename):
    try:
        with open(filename, 'r') as file:
            for line in file:
                print(line)
            return line
    except FileNotFoundError:
        print(f"File '{filename}' not found.")
        return None


reservas = read_text_file('ProyectoBDA/data_Prim_ord/text/reservas.txt')

print(reservas)        
        

