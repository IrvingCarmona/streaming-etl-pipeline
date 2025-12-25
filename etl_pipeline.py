"""
PROYECTO: Pipeline ETL de Streaming con Databricks y MongoDB
AUTOR: [Tu Nombre Completo]
FECHA: Diciembre 2025
DESCRIPCIÓN: 
    Este script extrae logs crudos de un sistema de archivos, 
    los transforma usando PySpark para limpieza y análisis, 
    y carga los resultados en un Data Lake (Parquet) y una base NoSQL (MongoDB).
"""

import pymongo
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- CONFIGURACIÓN ---
# En un entorno real, estas credenciales irían en variables de entorno o secrets
MONGO_URI = "mongodb+srv://admin:TU_CONTRASEÑA@cluster0....."  # <--- ¡PON TU LINK AQUÍ!
DB_NAME = "tienda_nube"
COLLECTION_NAME = "top_contenidos_streaming"

def extract_data(spark):
    """Genera/Extrae los datos crudos (Simulación de Ingesta)."""
    print("🔄 [EXTRACT] Generando datos crudos en memoria...")
    datos_sucios = [
        (101, "Stranger Things", 50, "MX"),
        (102, "Breaking Bad", 45, "MX"),
        (103, "Stranger Things", 10, "US"),
        (104, None, 0, None),              # Data Quality Issue
        (105, "One Piece", 25, "JP"),
        (106, "Stranger Things", 55, "MX"),
        (107, "Breaking Bad", 40, "US"),
        (108, "One Piece", 120, "JP")
    ]
    columnas = ["user_id", "contenido", "minutos_vistos", "pais"]
    return spark.createDataFrame(datos_sucios, schema=columnas)

def transform_data(df_raw):
    """Limpia y procesa los datos."""
    print("🔄 [TRANSFORM] Limpiando y agregando datos...")
    # Regla de negocio: Eliminar registros sin contenido o con 0 minutos
    df_clean = df_raw.filter("minutos_vistos > 0 AND contenido IS NOT NULL")
    return df_clean

def load_datalake(df_clean):
    """Guarda el histórico en formato optimizado Parquet."""
    print("🔄 [LOAD] Guardando en Data Lake (Parquet)...")
    df_clean.write.mode("overwrite").saveAsTable("tabla_streaming_clean")
    print("✅ Guardado exitoso en el Lakehouse.")

def load_nosql(df_clean):
    """Calcula KPIs y los envía a MongoDB para consumo en tiempo real."""
    print("🔄 [LOAD] Preparando envío a MongoDB...")
    
    # Agregación: Top contenidos
    df_kpi = df_clean.groupBy("contenido").count().withColumnRenamed("count", "vistas")
    
    # Convertir a formato nativo de Python (Lista de Diccionarios)
    data_mongo = [row.asDict() for row in df_kpi.collect()]
    
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        coll = db[COLLECTION_NAME]
        
        # Estrategia de carga: Truncate and Load (Borrar anterior y cargar nuevo)
        coll.delete_many({})
        coll.insert_many(data_mongo)
        print(f"🚀 [SUCCESS] {len(data_mongo)} documentos enviados a MongoDB Atlas.")
        client.close()
    except Exception as e:
        print(f"❌ [ERROR] Falló la conexión a Mongo: {e}")

# --- PUNTO DE ENTRADA PRINCIPAL (MAIN) ---
if __name__ == "__main__":
    # Inicializar Spark (Ya viene listo en Databricks, pero es buena práctica declararlo)
    spark = SparkSession.builder.appName("StreamingETL").getOrCreate()
    
    # Ejecución del Pipeline
    df_raw = extract_data(spark)
    df_clean = transform_data(df_raw)
    load_datalake(df_clean)
    load_nosql(df_clean)