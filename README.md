# streaming-etl-pipeline
Este script extrae logs crudos de un sistema de archivos, los transforma usando PySpark para limpieza y análisis,y carga los resultados en un Data Lake (Parquet) y una base NoSQL (MongoDB).
#  Streaming Data Pipeline ETL

## Descripción
Este proyecto implementa un pipeline de Ingeniería de Datos "End-to-End" para una plataforma de streaming. El sistema ingesta logs de usuarios, procesa la información utilizando **Apache Spark** para limpieza y control de calidad, y distribuye los resultados a dos destinos:
1. **Data Lake (Parquet):** Para almacenamiento histórico y auditoría.
2. **MongoDB (NoSQL):** Para alimentar un dashboard de "Top Contenidos" en tiempo real.

##  Tecnologías Utilizadas
* **Lenguaje:** Python 3.9
* **Procesamiento:** PySpark (Spark SQL & DataFrames)
* **Infraestructura:** Databricks Community Edition
* **Base de Datos NoSQL:** MongoDB Atlas
* **Orquestación:** Databricks Workflows

##  Arquitectura del Flujo
1. **Extract:** Generación/Ingesta de datos crudos (logs de visualización).
2. **Transform:** Filtrado de datos corruptos (Nulls, tiempos erróneos) y agregación por contenido.
3. **Load:** Carga híbrida hacia Data Lake (Analítica) y MongoDB (Operacional).

##  Autor
**[Irving Carmona ]** *Ingeniero de Datos en formación* 📧 [ing.carmona.irving@gmail.com]  
🔗 [Link a tu LinkedIn si tienes]
