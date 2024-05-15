from pyspark.sql import SparkSession

def perform_data_analysis():
    spark = SparkSession.builder \
        .appName("ReservasDataAnalysis") \
        .config("spark.driver.extraClassPath", "/path/to/postgresql/jdbc/driver") \
        .getOrCreate()

    # Conexión con la base de datos PostgreSQL
    properties = {"user": "primOrd", "password": "bdaPrimOrd"}
    url = "jdbc:postgresql://localhost:9999/PrimOrd"

    
    # ¿Quiénes son los empleados que trabajan en cada restaurante, junto con sus cargos y fechas de contratación?
    spark.sql("SELECT e.posicion, e.fecha_contratacion, e.nombre FROM empleados e INNER JOIN restaurante r ON r.id_restaurante = e.id_restaurante;").show()  

   
    spark.stop()

if __name__ == "__main__":
    perform_data_analysis()