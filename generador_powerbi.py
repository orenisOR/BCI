import json
import os
import csv
import re
from datetime import datetime

def parse_llm_json(texto_llm):
    """Limpia el markdown y convierte la cadena a dict de Python."""
    if not texto_llm:
        return {}
    cleaned = re.sub(r"```json\n?|```\n?", "", texto_llm.strip())
    try:
        return json.loads(cleaned)
    except Exception:
        return {}

def es_entorno_databricks():
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ or 
        "DATABRICKS_APP_PORT" in os.environ or
        "DATABRICKS_APP_NAME" in os.environ
    )

def cargar_datos_hibridos(archivo_resultados="mock_kqi_resultados.json"):
    is_db = es_entorno_databricks()
    datos = []
    config_global = {}
    
    if is_db:
        # Intentar cargar usando PySpark (si está disponible y estamos en Databricks)
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            # Cargar resultados
            df_res = spark.read.table("bci_kqi_apolo.kqi_resultados")
            # Convertir rows a dicts
            datos = [row.asDict() for row in df_res.collect()]
            
            # Cargar config
            df_config = spark.read.table("bci_kqi_apolo.kqi_configuracion")
            rows = df_config.collect()
            for row in rows:
                nombre = row.nombre_tipo_documento
                tipo_proc = row.tipo_procesamiento
                params = json.loads(row.parametros_json) if row.parametros_json else {}
                fecha = str(row.fecha_modificacion) if row.fecha_modificacion else ""
                config_global[nombre] = {
                    "tipo_procesamiento": tipo_proc,
                    "parametros": params,
                    "timestamp": fecha
                }
            print("✅ Cargados resultados y configuración desde Spark Tables.")
            return datos, config_global
        except Exception as e:
            print(f"Aviso: Error cargando desde Spark: {e}. Intentando vía SQL connector...")
            
        # Si Spark falla, intentar vía databricks-sql-connector
        try:
            from utils import obtener_conexion_databricks
            conn = obtener_conexion_databricks()
            cursor = conn.cursor()
            
            # Cargar resultados
            cursor.execute("SELECT * FROM bci_kqi_apolo.kqi_resultados")
            columns = [desc[0] for desc in cursor.description]
            datos = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Cargar config
            cursor.execute("SELECT nombre_tipo_documento, tipo_procesamiento, parametros_json, fecha_modificacion FROM bci_kqi_apolo.kqi_configuracion")
            rows = cursor.fetchall()
            for row in rows:
                nombre = row[0]
                tipo_proc = row[1]
                params = json.loads(row[2]) if row[2] else {}
                fecha = str(row[3]) if row[3] else ""
                config_global[nombre] = {
                    "tipo_procesamiento": tipo_proc,
                    "parametros": params,
                    "timestamp": fecha
                }
                
            cursor.close()
            conn.close()
            print("✅ Cargados resultados y configuración desde SQL Warehouse.")
            return datos, config_global
        except Exception as e:
            print(f"Aviso: Error cargando desde SQL Connector: {e}. Usando fallback local...")

    # Fallback local
    try:
        with open(archivo_resultados, 'r', encoding='utf-8') as f:
            datos = json.load(f)
    except FileNotFoundError:
        print(f"Aviso: No se encontró el archivo local {archivo_resultados}.")
        
    try:
        with open("mock_config.json", "r", encoding="utf-8") as f:
            config_global = json.load(f)
    except Exception:
        config_global = {}
        
    return datos, config_global

def generar_vistas_powerbi(archivo_resultados="mock_kqi_resultados.json", directorio_salida="powerbi_data"):
    if not os.path.exists(directorio_salida):
        os.makedirs(directorio_salida)

    datos, config_global = cargar_datos_hibridos(archivo_resultados)
    
    if not datos:
        print("Error: No se encontraron datos para generar reportes PowerBI.")
        return

    # Estructuras para agregación
    resumen_proyectos = {}
    semaforo_datos = []
    
    # 1. Procesar cada documento para extraer el semáforo y agrupar por proyecto
    for doc in datos:
        doc_lower = {k.lower(): v for k, v in doc.items()}
        proyecto = doc_lower.get("nombre_documento", "Desconocido")
        score = doc_lower.get("score_global", 0.0)
        id_doc = doc_lower.get("id_documento", "SinID")
        
        # Obtener umbrales dinámicos
        proyecto_config = config_global.get(proyecto, {}).get("parametros", {}).get("umbrales", {})
        umbral_gold = proyecto_config.get("aprobacion_gold", 0.80)
        semaforo_verde = proyecto_config.get("semaforo_verde", 0.80)
        semaforo_amarillo = proyecto_config.get("semaforo_amarillo", 0.50)
        
        # Nuevos campos de Bases Técnicas
        tokens_in = doc_lower.get("tokens_in") or 0
        tokens_out = doc_lower.get("tokens_out") or 0
        costo_usd = doc_lower.get("costo_estimado_usd") or 0.0
        pii_json = doc_lower.get("hallazgos_pii", "{}")
        try:
            pii_datos = json.loads(pii_json) if isinstance(pii_json, str) else pii_json
        except Exception:
            pii_datos = {}
            
        ruts = pii_datos.get("rut_chileno", 0)
        correos = pii_datos.get("correo", 0)
        telefonos = pii_datos.get("telefono", 0)
        
        # Inicializar proyecto en el resumen si no existe
        if proyecto not in resumen_proyectos:
            resumen_proyectos[proyecto] = {
                "total_documentos": 0,
                "suma_scores": 0.0,
                "documentos_con_discrepancias": 0,
                "total_tokens_in": 0,
                "total_tokens_out": 0,
                "costo_total_usd": 0.0
            }
            
        # Agregación para el Resumen
        resumen_proyectos[proyecto]["total_documentos"] += 1
        resumen_proyectos[proyecto]["suma_scores"] += score
        resumen_proyectos[proyecto]["total_tokens_in"] += tokens_in
        resumen_proyectos[proyecto]["total_tokens_out"] += tokens_out
        resumen_proyectos[proyecto]["costo_total_usd"] += costo_usd
        
        if score < umbral_gold:
            resumen_proyectos[proyecto]["documentos_con_discrepancias"] += 1
            
        # Extracción para Semáforo de Indicadores
        metricas_dict = parse_llm_json(doc_lower.get("detalle_error_llm", ""))
        
        for metrica, valor_orig in metricas_dict.items():
            # Estandarizar valor para que 1.0 siempre sea "Excelente" y 0.0 "Pésimo"
            if metrica in ["ambiguedad", "palabras_ilegibles", "ratio_imagenes"]:
                valor_ajustado = 1.0 - float(valor_orig)
            else:
                # Coherencia y Entropía: Valores más altos son mejores
                valor_ajustado = float(valor_orig)
                
            # Calcular Semáforo parametrizado
            if valor_ajustado >= semaforo_verde:
                color = "Verde"
            elif valor_ajustado >= semaforo_amarillo:
                color = "Amarillo"
            else:
                color = "Rojo"
                
            semaforo_datos.append({
                "id_documento": id_doc,
                "nombre_proyecto": proyecto,
                "kqi_metrica": metrica,
                "valor_original": round(float(valor_orig), 2),
                "valor_estandarizado": round(valor_ajustado, 2),
                "semaforo_color": color,
                "pii_ruts": ruts,
                "pii_correos": correos,
                "pii_telefonos": telefonos
            })
            
    # 2. Generar CSV de Resumen General (Tabla 1)
    ruta_resumen = os.path.join(directorio_salida, "01_resumen_proyectos.csv")
    with open(ruta_resumen, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "nombre_proyecto", 
            "fecha_corte", 
            "total_documentos_analizados", 
            "salud_general_proyecto_porcentaje", 
            "discrepancias_no_resueltas",
            "total_tokens_entrada",
            "total_tokens_salida",
            "costo_total_estimado_usd"
        ])
        
        fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for proy, agg in resumen_proyectos.items():
            salud_promedio = agg["suma_scores"] / agg["total_documentos"] if agg["total_documentos"] > 0 else 0
            writer.writerow([
                proy,
                fecha_actual,
                agg["total_documentos"],
                round(salud_promedio * 100, 2), # Expresado en base 100 para BI
                agg["documentos_con_discrepancias"],
                agg["total_tokens_in"],
                agg["total_tokens_out"],
                round(agg["costo_total_usd"], 6)
            ])
            
    # 3. Generar CSV de Semáforo KQIs (Tabla 2)
    ruta_semaforo = os.path.join(directorio_salida, "02_semaforo_kqis.csv")
    with open(ruta_semaforo, 'w', newline='', encoding='utf-8') as f:
        if len(semaforo_datos) > 0:
            writer = csv.DictWriter(f, fieldnames=semaforo_datos[0].keys())
            writer.writeheader()
            writer.writerows(semaforo_datos)
            
    # 4. Generar CSV de Alertas (Tabla 3)
    ruta_alertas = os.path.join(directorio_salida, "03_alertas_lote.csv")
    with open(ruta_alertas, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["nombre_proyecto", "fecha_alerta", "score_promedio", "umbral_critico"])
        
        alertas_dir = "alertas"
        if os.path.exists(alertas_dir):
            for arch in os.listdir(alertas_dir):
                if arch.endswith(".json"):
                    with open(os.path.join(alertas_dir, arch), "r", encoding="utf-8") as fa:
                        data = json.load(fa)
                        fecha = datetime.fromtimestamp(data["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
                        writer.writerow([data["proyecto"], fecha, round(data["score_promedio"] * 100, 2), round(data["umbral"] * 100, 2)])

            
    print(f"[OK] Vistas de PowerBI generadas exitosamente en el directorio '{directorio_salida}/'")
    print(f" -> {ruta_resumen}")
    print(f" -> {ruta_semaforo}")

if __name__ == "__main__":
    generar_vistas_powerbi()
