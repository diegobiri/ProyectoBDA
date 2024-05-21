from pyspark.sql import SparkSession

def perform_data_analysis():
    spark = SparkSession.builder \
        .appName("ReservasDataAnalysis") \
        .config("spark.driver.extraClassPath", "/path/to/postgresql/jdbc/driver") \
        .getOrCreate()

    # Conexión con la base de datos PostgreSQL
    properties = {"user": "primOrd", "password": "bdaPrimOrd"}
    url = "jdbc:postgresql://localhost:9999/PrimOrd"

    # Cargar tablas desde PostgreSQL
    menus_df = spark.read.jdbc(url=url, table="menus", properties=properties)
    clientes_df = spark.read.jdbc(url=url, table="clientes", properties=properties)
    platos_df = spark.read.jdbc(url=url, table="platos", properties=properties)
    reservas_df = spark.read.jdbc(url=url, table="reservas", properties=properties)

    # Registrar DataFrames como tablas SQL temporales
    menus_df.createOrReplaceTempView("menus")
    clientes_df.createOrReplaceTempView("clientes")
    platos_df.createOrReplaceTempView("platos")
    reservas_df.createOrReplaceTempView("reservas")

    # Consultas especificadas

    # 5.2.1 Análisis de las preferencias de los clientes
    # ¿Cuáles son las preferencias alimenticias más comunes entre los clientes?
    preferencias_clientes_df = spark.sql("""
        SELECT preferencias_alimenticias, COUNT(*) AS count
        FROM reservas
        GROUP BY preferencias_alimenticias
        ORDER BY count DESC
    """)
    preferencias_clientes_df.show()

    # 5.2.2 Análisis del rendimiento del restaurante
    # ¿Qué restaurante tiene el precio medio de menú más alto?
    precio_medio_menu_df = spark.sql("""
        SELECT r.nombre AS nombre_restaurante, AVG(m.precio) AS precio_medio
        FROM menus m
        JOIN reservas r ON m.id_restaurante = r.id_restaurante
        GROUP BY r.id_restaurante
        ORDER BY precio_medio DESC
        LIMIT 1
    """)
    precio_medio_menu_df.show()

    # ¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?
    disponibilidad_platos_df = spark.sql("""
        SELECT r.nombre AS nombre_restaurante, m.disponibilidad, COUNT(*) AS count
        FROM menus m
        JOIN restaurantes r ON m.id_restaurante = r.id_restaurante
        GROUP BY r.nombre, m.disponibilidad
        ORDER BY r.nombre, m.disponibilidad DESC
    """)
    disponibilidad_platos_df.show()

    # 5.2.3 Patrones de reserva
    # ¿Cuál es la duración media de la estancia de los clientes de un hotel?
    duracion_media_estancia_df = spark.sql("""
        SELECT h.nombre_hotel, AVG(DATEDIFF(r.fecha_salida, r.fecha_llegada)) AS duracion_media_estancia
        FROM reservas r
        JOIN hoteles h ON r.id_restaurante = h.id_hotel
        GROUP BY h.nombre_hotel
    """)
    duracion_media_estancia_df.show()

    # ¿Existen periodos de máxima ocupación en función de las fechas de reserva?
    maxima_ocupacion_df = spark.sql("""
        SELECT DATE_TRUNC('month', fecha_llegada) AS mes, COUNT(*) AS numero_reservas
        FROM reservas
        GROUP BY mes
        ORDER BY numero_reservas DESC
    """)
    maxima_ocupacion_df.show()

   

    

    # ¿Podemos estimar los ingresos generados por cada hotel basándonos en los precios de las habitaciones y los índices de ocupación?
    ingresos_est_df = spark.sql("""
        SELECT h.nombre_hotel, 
               SUM(hab.tarifa_por_noche * DATEDIFF(r.fecha_salida, r.fecha_llegada)) AS ingresos_est
        FROM reservas r
        JOIN habitaciones hab ON r.id_habitacion = hab.numero_habitacion
        JOIN hoteles h ON hab.numero_habitacion = ANY(h.empleados)
        GROUP BY h.nombre_hotel
    """)
    ingresos_est_df.show()

    # 5.2.6 Análisis de menús
    # ¿Qué platos son los más y los menos populares entre los restaurantes?
    popularidad_platos_df = spark.sql("""
        SELECT p.nombre AS nombre_plato, COUNT(r.id_plato) AS numero_veces_ordenado
        FROM platos p
        JOIN relaciones r ON p.platoID = r.id_plato
        GROUP BY p.nombre
        ORDER BY numero_veces_ordenado DESC
    """)
    popularidad_platos_df.show()

    # ¿Hay ingredientes o alérgenos comunes que aparezcan con frecuencia en los platos?
    ingredientes_comunes_df = spark.sql("""
        SELECT UNNEST(string_to_array(ingredientes, ',')) AS ingrediente, COUNT(*) AS frecuencia
        FROM platos
        GROUP BY ingrediente
        ORDER BY frecuencia DESC
    """)
    ingredientes_comunes_df.show()

    alergenos_comunes_df = spark.sql("""
        SELECT UNNEST(string_to_array(alergenos, ',')) AS alérgeno, COUNT(*) AS frecuencia
        FROM platos
        GROUP BY alérgeno
        ORDER BY frecuencia DESC
    """)
    alergenos_comunes_df.show()

    # 5.2.7 Comportamiento de los clientes
    # ¿Existen pautas en las preferencias de los clientes en función de la época del año?
    pautas_clientes_df = spark.sql("""
        SELECT DATE_TRUNC('month', r.fecha_llegada) AS mes, c.preferencias_alimenticias, COUNT(*) AS count
        FROM reservas r
        JOIN clientes c ON r.idCliente = c.id_cliente
        GROUP BY mes, c.preferencias_alimenticias
        ORDER BY mes, count DESC
    """)
    pautas_clientes_df.show()

    # ¿Los clientes con preferencias dietéticas específicas tienden a reservar en restaurantes concretos?
    reservas_especificas_df = spark.sql("""
        SELECT c.preferencias_alimenticias, r.nombre AS nombre_restaurante, COUNT(*) AS count
        FROM reservas res
        JOIN clientes c ON res.idCliente = c.id_cliente
        JOIN restaurantes r ON res.id_restaurante = r.id_restaurante
        GROUP BY c.preferencias_alimenticias, r.nombre
        ORDER BY count DESC
    """)
    reservas_especificas_df.show()

    # 5.2.8 Garantía de calidad
    # ¿Existen discrepancias entre la disponibilidad de platos comunicada y las reservas reales realizadas?
    discrepancias_platos_df = spark.sql("""
        SELECT m.id_menu, m.disponibilidad, COUNT(r.id_plato) AS reservas_reales
        FROM menus m
        JOIN relaciones r ON m.id_menu = r.id_menu
        GROUP BY m.id_menu, m.disponibilidad
        HAVING m.disponibilidad != COUNT(r.id_plato)
    """)
    discrepancias_platos_df.show()

    # 5.2.9 Análisis de mercado
    # ¿Cómo se comparan los precios de las habitaciones de los distintos hoteles y existen valores atípicos?
    precios_habitaciones_df = spark.sql("""
        SELECT h.nombre_hotel, hab.tarifa_por_noche
        FROM habitaciones hab
        JOIN hoteles h ON hab.numero_habitacion = ANY(h.empleados)
        ORDER BY hab.tarifa_por_noche DESC
    """)
    precios_habitaciones_df.show()


    spark.stop()

if __name__ == "__main__":
    perform_data_analysis()