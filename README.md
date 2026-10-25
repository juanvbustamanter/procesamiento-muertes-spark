# Procesamiento batch de muertes por eventos de transporte en Colombia

## Descripción del proyecto

Este proyecto realiza un procesamiento batch de datos sobre muertes por eventos de transporte en Colombia durante el periodo 2015-2024, utilizando Python y Apache Spark.

El objetivo es explorar los registros y analizar su distribución por año, mes y sexo, con el fin de identificar patrones descriptivos en los datos.

## Fuente de datos

Los datos provienen del conjunto de datos **Muertes por eventos de transporte, Colombia, años 2015 a 2024. Cifras definitivas**, publicado en el portal de Datos Abiertos de Colombia.

[Consultar el conjunto de datos](https://www.datos.gov.co/Justicia-y-Derecho/Muertes-por-eventos-de-transporte-Colombia-a-os-20/s65h-7665)

## Tecnologías utilizadas

- Python.
- Apache Spark.
- PySpark.
- Pandas.
- Matplotlib.

## Archivos del repositorio

- `batch_muertes.py`: código fuente del procesamiento batch.
- `grafico_anual.png`: visualización de registros por año.
- `grafico_mensual.png`: visualización de registros por mes.
- `grafico_sexo.png`: visualización de registros por sexo.
- `registros_por_anio.csv`: resultados agrupados por año.
- `registros_por_mes.csv`: resultados agrupados por mes.
- `registros_por_sexo.csv`: resultados agrupados por sexo.

## Requisitos

Para ejecutar el proyecto se necesita:

- Python 3.
- Apache Spark y PySpark.
- Pandas.
- Matplotlib.
- El archivo CSV original del conjunto de datos.

## Instrucciones de ejecución

1. Descargue el conjunto de datos desde la fuente indicada.
2. Guarde el archivo CSV en la ubicación esperada por el script o ajuste la ruta de entrada en `batch_muertes.py`.
3. Verifique que las dependencias estén instaladas.
4. Ejecute el script desde la terminal de Ubuntu:

   `python3 batch_muertes.py`

5. Revise los gráficos y archivos CSV generados por el procesamiento.

## Resultados

El procesamiento genera tablas resumidas y gráficos para analizar la distribución de los registros por año, mes y sexo.

Los resultados permiten observar diferencias entre periodos y categorías demográficas, como parte de un análisis descriptivo del conjunto de datos.
