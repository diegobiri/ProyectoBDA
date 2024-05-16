claims = load 'reservas.csv' using PigStorage(',') as (
    ID Cliente:int, Fecha Llegada:timestamp, Fecha Salida:chararray, Tipo Habitacion:chararray, Preferencias Comida:chararray,
       ID Restaurante:int);


reservas_cliente_month = FOREACH claims GENERATE ID Cliente, GetMonth(Fecha Llegada) AS month, reservas_cliente;

reservas_cliente_por_month = GROUP reservas_cliente_month BY (ID Cliente, month);
Calcular el total de ventas para cada producto y mes:
total_reservas_por_cliente_month = FOREACH reservas_cliente_por_month GENERATE group.month AS month, COUNT(ID Cliente) AS total_sales_amount;
Almacenar el resultado:
STORE total_reservas_por_cliente_month INTO 'output';