from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt
import pandas as pd
import os
import shutil

# 1. Crear la sesion de Spark
spark = SparkSession.builder.appName("AnalisisMuertesTransporte").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Definir las rutas
archivo = "/home/vboxuser/muertes_transporte.csv"
carpeta_salida = "/home/vboxuser/resultados_batch"
os.makedirs(carpeta_salida, exist_ok=True)

print("\n=== PROCESAMIENTO BATCH: MUERTES POR EVENTOS DE TRANSPORTE ===")

# 3. Cargar el archivo CSV
df = spark.read.csv(archivo, header=True, inferSchema=True)

print("\n=== ESTRUCTURA DEL DATASET ===")
print("Registros originales:", df.count())
print("Columnas:", len(df.columns))
df.printSchema()

# 4. Revisar valores nulos
print("\n=== VALORES NULOS POR COLUMNA ===")
nulos = df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
])
nulos.show(vertical=True, truncate=False)

# 5. Revisar duplicados exactos
duplicados = df.count() - df.dropDuplicates().count()
print("Duplicados exactos:", duplicados)

# 6. Limpiar y preparar los datos
df_limpio = df.withColumn(
    "mes_del_hecho",
    F.initcap(F.lower(F.trim(F.col("mes_del_hecho"))))
)

print("\n=== MESES NORMALIZADOS ===")
df_limpio.select("mes_del_hecho").distinct().orderBy(
    "mes_del_hecho"
).show(20, truncate=False)

# 7. Analisis por año
print("\n=== REGISTROS POR AÑO ===")
anual = df_limpio.groupBy("a_o_del_hecho").count().orderBy(
    "a_o_del_hecho"
)
anual.show()

# 8. Analisis por mes
print("\n=== REGISTROS POR MES ===")
mensual = df_limpio.groupBy("mes_del_hecho").count().orderBy(
    "mes_del_hecho"
)
mensual.show(20, truncate=False)

# 9. Analisis por departamento
print("\n=== DIEZ DEPARTAMENTOS CON MAS REGISTROS ===")
departamentos = df_limpio.groupBy(
    "departamento_del_hecho_dane"
).count().orderBy(F.desc("count"))
departamentos.show(10, truncate=False)

# 10. Analisis por sexo
print("\n=== DISTRIBUCION POR SEXO ===")
total = df_limpio.count()

sexo = df_limpio.groupBy("sexo_de_la_victima").count().withColumn(
    "porcentaje",
    F.round(F.col("count") / total * 100, 2)
).orderBy(F.desc("count"))

sexo.show(truncate=False)

# 11. Convertir las tablas agregadas a Pandas
datos_anuales = anual.toPandas()
datos_mensuales = mensual.toPandas()
datos_sexo = sexo.toPandas()

# 12. Generar grafico anual
plt.figure(figsize=(10, 6))
plt.bar(datos_anuales["a_o_del_hecho"], datos_anuales["count"])
plt.title("Muertes por eventos de transporte en Colombia (2015-2024)")
plt.xlabel("Año")
plt.ylabel("Cantidad de registros")
plt.xticks(datos_anuales["a_o_del_hecho"])
plt.tight_layout()
plt.savefig(
    os.path.join(carpeta_salida, "grafico_anual.png"),
    dpi=300
)
plt.close()

# 13. Generar grafico mensual
orden_meses = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre",
    "Diciembre", "Sin informacion", "Sin información"
]

datos_mensuales["mes_del_hecho"] = datos_mensuales["mes_del_hecho"].str.strip().str.capitalize()
datos_mensuales["mes_del_hecho"] = datos_mensuales["mes_del_hecho"].replace("Sin informacion", "Sin Información")
datos_mensuales["mes_del_hecho"] = pd.Categorical(
    datos_mensuales["mes_del_hecho"],
    categories=orden_meses,
    ordered=True
)
datos_mensuales = datos_mensuales.sort_values("mes_del_hecho")

plt.figure(figsize=(11, 6))
plt.bar(datos_mensuales["mes_del_hecho"].astype(str), datos_mensuales["count"])
plt.title("Registros por mes")
plt.xlabel("Mes")
plt.ylabel("Cantidad de registros")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(
    os.path.join(carpeta_salida, "grafico_mensual.png"),
    dpi=300
)
plt.close()

# 14. Generar grafico por sexo
plt.figure(figsize=(8, 6))
plt.bar(datos_sexo["sexo_de_la_victima"], datos_sexo["count"])
plt.title("Registros por sexo de la victima")
plt.xlabel("Sexo")
plt.ylabel("Cantidad de registros")
plt.tight_layout()
plt.savefig(
    os.path.join(carpeta_salida, "grafico_sexo.png"),
    dpi=300
)
plt.close()

# 15. Guardar tablas de resultados
anual.toPandas().to_csv(
    os.path.join(carpeta_salida, "registros_por_anio.csv"),
    index=False
)
mensual.toPandas().to_csv(
    os.path.join(carpeta_salida, "registros_por_mes.csv"),
    index=False
)
sexo.toPandas().to_csv(
    os.path.join(carpeta_salida, "registros_por_sexo.csv"),
    index=False
)

# 16. Copiar archivos generados a carpeta compartida en Windows
# Copiar resultados a la carpeta compartida con Windows
carpeta_compartida = "/mnt/Resultados_Spark"

os.makedirs(carpeta_compartida, exist_ok=True)

for archivo in os.listdir(carpeta_salida):
    ruta_origen = os.path.join(carpeta_salida, archivo)
    ruta_destino = os.path.join(carpeta_compartida, archivo)

    if os.path.isfile(ruta_origen):
        shutil.copy2(ruta_origen, ruta_destino)

print("Resultados copiados a la carpeta compartida con Windows.")

print("\n=== ARCHIVOS GENERADOS ===")
print("Carpeta:", carpeta_salida)
print("Grafico anual: grafico_anual.png")
print("Grafico mensual: grafico_mensual.png")
print("Grafico por sexo: grafico_sexo.png")
print("Tablas CSV: registros_por_anio.csv, registros_por_mes.csv, registros_por_sexo.csv")

print("\nProcesamiento batch finalizado correctamente.")

spark.stop()
