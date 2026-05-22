import sys
import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, lit, when, udf
from pyspark.sql.types import StringType, FloatType
from dotenv import load_dotenv

is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

if not is_databricks:
    # Forzar ejecutable de Python para los workers de Spark en Windows
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Auto-detectar y configurar JAVA_HOME compatible (Java 11, 17, 21) en Windows si está configurada una versión antigua (Java 8)
    if os.name == 'nt':
        java_home = os.environ.get('JAVA_HOME', '')
        if not java_home or 'jdk1.8' in java_home.lower() or 'jre8' in java_home.lower():
            corretto_dir = r"C:\Program Files\Amazon Corretto"
            rutas_posibles = [
                r"C:\Program Files\Amazon Corretto\jdk21.0.11_10"
            ]
            if os.path.exists(corretto_dir):
                for item in os.listdir(corretto_dir):
                    path = os.path.join(corretto_dir, item)
                    # Spark 3.5+ soporta oficialmente hasta Java 21. Java 25 (jdk25) causa fallos getSubject en Hadoop.
                    if os.path.isdir(path) and ("jdk11" in item.lower() or "jdk17" in item.lower() or "jdk21" in item.lower()):
                        rutas_posibles.insert(0, path)
            
            for ruta in rutas_posibles:
                if os.path.exists(ruta):
                    os.environ['JAVA_HOME'] = ruta
                    os.environ['PATH'] = os.path.join(ruta, 'bin') + os.pathsep + os.environ.get('PATH', '')
                    break

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MotorKQI")

class SmartRouter:
    def __init__(self):
        # Intentamos inicializar Spark. Si falla (por falta de Java/Winutils en local), manejamos la excepción.
        try:
            self.spark = SparkSession.builder \
                .appName("MotorKQI_Apolo") \
                .getOrCreate()
            logger.info("Spark Session inicializada correctamente.")
        except Exception as e:
            logger.error(f"Error inicializando Spark: {e}")
            self.spark = None

    def obtener_api_key(self):
        """Obtiene la API Key de Gemini desde Databricks Secrets o local .env."""
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            try:
                # Intentar obtener dbutils de la sesión Spark
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(self.spark)
                key = dbutils.secrets.get(scope="bci-apolo-secrets", key="gemini-api-key")
                if key:
                    logger.info("API Key de Gemini cargada exitosamente desde Databricks Secrets.")
                    return key
            except Exception as e:
                logger.warning(f"No se pudo leer de Databricks Secrets: {e}. Usando fallback local...")
        return os.environ.get("GOOGLE_API_KEY")
    
    def procesar_semiestructurado(self, df, reglas):
        """
        Ruta Semiestructurada: Validación determinista de reglas matemáticas sin usar LLMs.
        Se asume que el JSON ya fue aplanado previamente.
        """
        logger.info("Iniciando procesamiento Semiestructurado.")
        
        if self.spark is None or df is None:
            logger.warning("[MOCK] Spark no disponible o DataFrame vacío.")
            return None
        
        from pyspark.sql.functions import array, concat_ws, lit, when, col
        from pyspark.sql.types import StringType

        if not reglas:
            condicion_global = lit(True)
            errores_rutas = [lit(None).cast(StringType())]
        else:
            condiciones_validas = []
            errores_rutas = []
            
            for regla in reglas:
                atributo = regla.get("atributo")
                operador = regla.get("operador")
                valor = regla.get("valor")
                
                # Intentar castear el valor numérico
                try:
                    if "." in str(valor):
                        val_lit = float(valor)
                    else:
                        val_lit = int(valor)
                except ValueError:
                    val_lit = valor # String
                
                c = col(atributo)
                v = lit(val_lit)
                
                if operador == ">":
                    cond = c > v
                elif operador == "<":
                    cond = c < v
                elif operador == ">=":
                    cond = c >= v
                elif operador == "<=":
                    cond = c <= v
                elif operador == "==":
                    cond = c == v
                elif operador == "!=":
                    cond = c != v
                else:
                    cond = lit(True)
                    
                condiciones_validas.append(cond)
                # Si no se cumple la condición, guardamos la ruta del atributo
                errores_rutas.append(when(~cond, lit(f"$.{atributo}")).otherwise(lit(None).cast(StringType())))
                
            # Combinamos las condiciones (AND)
            condicion_global = condiciones_validas[0]
            for c in condiciones_validas[1:]:
                condicion_global = condicion_global & c

        # Concatenamos los errores, omitiendo nulos automáticamente gracias a concat_ws
        ruta_col = concat_ws(", ", array(*errores_rutas))
        ruta_col = when(ruta_col == "", lit(None).cast(StringType())).otherwise(ruta_col)

        df_resultado = df.withColumn("tipo_procesamiento", lit("Semiestructurado")) \
                         .withColumn("score_global", when(condicion_global, 1.0).otherwise(0.0)) \
                         .withColumn("detalle_error_llm", lit(None).cast(StringType())) \
                         .withColumn("ruta_nodo_json_error", ruta_col)
        
        return df_resultado

    @staticmethod
    def llamar_gemini(texto, parametros):
        """
        Función que llama a la API de Gemini (o simula la llamada si no hay API Key)
        """
        api_key = parametros.get("api_key") or os.environ.get("GOOGLE_API_KEY")
        # En Databricks, esto sería: api_key = dbutils.secrets.get(scope="my_scope", key="gemini_api_key")
        
        modelo_llm = parametros.get("modelo_llm", "gemini-1.5-pro")
        variables_kqi = list(parametros.get("pesos_kqi", {}).keys())
        if not variables_kqi:
            variables_kqi = ["Coherencia", "Ambiguedad", "Entropia"] # fallback
            
        variables_str = "', '".join(variables_kqi)
        
        if not api_key:
            logger.warning("[MOCK LLM] API Key no encontrada. Simulando respuesta de Gemini.")
            mock_response = {v: 0.5 for v in variables_kqi}
            mock_response["Comentarios"] = "Texto simulado evaluado exitosamente."
            return json.dumps(mock_response)
            
        try:
            from google import genai
            client = genai.Client(api_key=api_key)
            prompt = f"Evalúa el siguiente texto devolviendo un JSON con '{variables_str}' entre 0.0 y 1.0: {texto}"
            response = client.models.generate_content(
                model=modelo_llm,
                contents=prompt,
            )
            
            tokens_in = response.usage_metadata.prompt_token_count if response.usage_metadata else 0
            tokens_out = response.usage_metadata.candidates_token_count if response.usage_metadata else 0
            
            import json
            resultado = {
                "respuesta_llm": response.text,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out
            }
            return json.dumps(resultado)
        except Exception as e:
            logger.error(f"Error al llamar a Gemini: {e}")
            return None

    @staticmethod
    def detectar_pii(texto, regex_dict):
        import re
        import json
        if not texto: return "{}"
        hallazgos = {}
        for pii_name, pattern in regex_dict.items():
            matches = re.findall(pattern, texto)
            hallazgos[pii_name] = len(matches)
        return json.dumps(hallazgos)

    @staticmethod
    def calcular_score(llm_response, pesos):
        if not llm_response:
            return 0.0
        try:
            import json
            # Limpiar formato markdown (```json ... ```)
            cleaned = llm_response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            
            data = json.loads(cleaned.strip())
            
            # Calcular promedio ponderado invirtiendo las variables negativas
            score_acumulado = 0.0
            total_peso = sum(pesos.values())
            if total_peso == 0: return 0.0
            
            for k, peso in pesos.items():
                val = float(data.get(k, 0.0))
                # Variables donde mayor valor = peor calidad
                if k in ["ambiguedad", "entropia", "palabras_ilegibles", "ratio_imagenes"]:
                    val_positivo = 1.0 - val
                else:
                    val_positivo = val # Variables donde mayor valor = mejor calidad (ej. coherencia)
                    
                score_acumulado += val_positivo * float(peso)
                
            score = score_acumulado / total_peso
            return float(round(score, 4))
        except Exception:
            return 0.0

    def procesar_no_estructurado(self, df, parametros):
        """
        Ruta No Estructurada: FAISS + LLM as a Judge (Gemini)
        """
        logger.info("Iniciando procesamiento No Estructurado.")
        
        try:
            import faiss
            import numpy as np
            # Simulación de Chunking y Base Vectorial Efímera
            dimension = 128 # Dimensión simulada de embeddings
            index = faiss.IndexFlatL2(dimension)
            # Agregar un vector dummy
            index.add(np.random.random((1, dimension)).astype('float32'))
            logger.info("Índice FAISS efímero creado y poblado en memoria.")
        except ImportError:
            logger.warning("FAISS no está instalado. Saltando indexación vectorial.")
            
        if self.spark is None or df is None:
            logger.warning("[MOCK] Spark no disponible o DataFrame vacío.")
            return None

        # Obtener API Key en el driver para evitar acceder a dbutils en los executors
        api_key = self.obtener_api_key()
        parametros_copy = dict(parametros)
        parametros_copy["api_key"] = api_key

        # UDF para llamar a Gemini en Spark
        gemini_udf = udf(lambda txt: SmartRouter.llamar_gemini(txt, parametros_copy), StringType())
        
        regex_dict = parametros.get("regex_pii", {})
        pii_udf = udf(lambda txt: SmartRouter.detectar_pii(txt, regex_dict), StringType())
        
        from pyspark.sql.functions import regexp_replace, get_json_object, when, avg
        
        df_temp = df.withColumn("tipo_procesamiento", lit("No Estructurado")) \
                    .withColumn("detalle_gemini", gemini_udf(col("texto_transcrito"))) \
                    .withColumn("hallazgos_pii", pii_udf(col("texto_transcrito")))
                    
        # CACHE crítico: Evita que Spark ejecute la UDF múltiples veces durante show(), collect(), etc.
        df_temp = df_temp.cache()
        df_temp.count() # Forzar materialización del caché
        
        # Extraer campos de Gemini y calcular costos
        df_temp = df_temp.withColumn("detalle_error_llm", get_json_object(col("detalle_gemini"), "$.respuesta_llm")) \
                         .withColumn("tokens_in", get_json_object(col("detalle_gemini"), "$.tokens_in").cast("int")) \
                         .withColumn("tokens_out", get_json_object(col("detalle_gemini"), "$.tokens_out").cast("int"))
                         
        # Costo aproximado Gemini 1.5 Flash (0.075 USD / 1M in, 0.30 USD / 1M out)
        df_temp = df_temp.withColumn("costo_estimado_usd", 
                                     (col("tokens_in") / 1000000.0 * 0.075) + 
                                     (col("tokens_out") / 1000000.0 * 0.30))
                    
        # Limpiar el formato markdown para que quede JSON puro
        cleaned_col = regexp_replace(col("detalle_error_llm"), "```json\\n?|```\\n?", "")
        
        pesos_dict = parametros.get("pesos_kqi", {})
        total_peso = sum(pesos_dict.values()) if sum(pesos_dict.values()) > 0 else 1.0
        
        # Construir la expresión de score dinámicamente en Spark SQL (Evita usar otro Python Worker)
        score_expr = lit(0.0)
        for k, peso in pesos_dict.items():
            val_expr = get_json_object(cleaned_col, f"$.{k}").cast("double")
            val_expr = when(val_expr.isNull(), 0.0).otherwise(val_expr)
            
            # NOTA: 'entropia' ya NO está en esta lista negra. Ahora suma positivo.
            if k in ["ambiguedad", "palabras_ilegibles", "ratio_imagenes"]:
                val_positivo = lit(1.0) - val_expr
            else:
                val_positivo = val_expr
                
            score_expr = score_expr + (val_positivo * lit(peso))
            
        score_expr = score_expr / lit(total_peso)
        
        df_resultado = df_temp.withColumn("score_global", score_expr) \
                              .withColumn("ruta_nodo_json_error", lit(None).cast(StringType()))
                              
        umbrales = parametros.get("umbrales", {})
        umbral_alerta = umbrales.get("alerta_lote_critico", 0.85)
        
        # Alerta de Lote
        try:
            promedio_lote = df_resultado.select(avg("score_global")).collect()[0][0]
            if promedio_lote is not None:
                if promedio_lote < umbral_alerta:
                    logger.warning(f"¡ALERTA DE GOBIERNO! El lote analizado tiene una confianza promedio de {promedio_lote:.2f}, menor al umbral del {umbral_alerta*100}% ({umbral_alerta}).")
                    
                    # Crear archivo de alerta físico
                    import os, time
                    if not os.path.exists("alertas"): os.makedirs("alertas")
                    import json
                    alerta_data = {
                        "proyecto": parametros.get("nombre_documento", "Desconocido"),
                        "timestamp": time.time(),
                        "score_promedio": promedio_lote,
                        "umbral": umbral_alerta
                    }
                    with open(f"alertas/alerta_lote_{int(time.time())}.json", "w", encoding="utf-8") as fa:
                        json.dump(alerta_data, fa, ensure_ascii=False)
                        
                else:
                    logger.info(f"Lote Saludable: Confianza promedio {promedio_lote:.2f}.")
        except Exception as e:
            logger.error(f"No se pudo calcular el promedio del lote para alertas: {e}")
                              
        return df_resultado

    def ingerir_y_aplanar(self, file_path="mock_data.json", is_db=False):
        """
        Módulo de Ingesta y Schema Normalization (Explode)
        """
        if self.spark is None:
            return None
            
        try:
            if is_db:
                df_raw = self.spark.read.table("bci_kqi_apolo.silver_transcripciones")
                logger.info("Ingesta desde Delta Table bci_kqi_apolo.silver_transcripciones completada.")
            else:
                df_raw = self.spark.read.option("multiline", "true").json(file_path)
                logger.info(f"Ingesta desde archivo local {file_path} completada.")
                
            # Supongamos que hay un arreglo anidado 'detalles'
            if "detalles" in df_raw.columns:
                df_flat = df_raw.withColumn("detalle_aplanado", explode_outer(col("detalles")))
                return df_flat
            return df_raw
        except Exception as e:
            logger.warning(f"No se pudo leer/aplanar {file_path if not is_db else 'bci_kqi_apolo.silver_transcripciones'}. Error: {e}")
            return None

    def ejecutar(self, nombre_documento=None):
        """
        Orquestador principal
        """
        is_db = "DATABRICKS_RUNTIME_VERSION" in os.environ
        
        # Intentamos leer la configuración guardada por la UI (Pilar 1)
        config_maestra = {}
        if is_db:
            try:
                # Leer desde tabla Spark SQL/Delta
                df_config = self.spark.read.table("bci_kqi_apolo.kqi_configuracion")
                rows = df_config.collect()
                for row in rows:
                    nombre = row.nombre_tipo_documento
                    tipo_proc = row.tipo_procesamiento
                    params = json.loads(row.parametros_json) if row.parametros_json else {}
                    fecha = str(row.fecha_modificacion) if row.fecha_modificacion else ""
                    config_maestra[nombre] = {
                        "tipo_procesamiento": tipo_proc,
                        "parametros": params,
                        "timestamp": fecha
                    }
                logger.info("Configuración maestra cargada exitosamente desde Delta Table bci_kqi_apolo.kqi_configuracion")
            except Exception as e:
                logger.warning(f"No se pudo cargar la configuración desde Delta Table: {e}. Usando fallback local.")
                is_db = False # Fallback a modo local
                
        if not config_maestra:
            try:
                with open("mock_config.json", "r", encoding="utf-8") as f:
                    config_maestra = json.load(f)
            except FileNotFoundError:
                logger.error("Archivo mock_config.json no encontrado. Ejecuta app.py primero.")
                return

        if not nombre_documento:
            # Para pruebas locales, si no se pasa argumento, tomamos el primero
            if config_maestra:
                nombre_documento = list(config_maestra.keys())[0]
                logger.info(f"No se especificó documento. Usando el primero disponible: {nombre_documento}")
            else:
                logger.error("No hay documentos configurados.")
                return

        config = config_maestra.get(nombre_documento)
        if not config:
            logger.error(f"El documento '{nombre_documento}' no existe en la configuración.")
            return

        tipo_proc = config.get("tipo_procesamiento")
        logger.info(f"Auditoría para: '{nombre_documento}' | Ruta detectada: {tipo_proc} | Entorno Databricks: {is_db}")
        
        # 1. Lectura desde Capa Plata (Silver)
        df_silver = self.ingerir_y_aplanar("mock_silver_data.json", is_db=is_db)
        if df_silver is None:
            logger.error("No se pudo leer la capa Plata.")
            return
            
        # Filtramos solo los documentos de este tipo
        df_silver_filtrado = df_silver.filter(col("nombre_documento") == nombre_documento)
        
        # 2. Evaluación KQI
        df_final = None
        if tipo_proc == "Semiestructurado":
            df_final = self.procesar_semiestructurado(df_silver_filtrado, config.get("parametros", {}).get("reglas", []))
        elif tipo_proc == "No Estructurado":
            df_final = self.procesar_no_estructurado(df_silver_filtrado, config.get("parametros", {}))
            
        if df_final:
            df_final = df_final.withColumn("nombre_tipo_documento", lit(nombre_documento))
            
            # Mostrar tabla procesada
            logger.info("DataFrame final tras la evaluación:")
            df_final.show(truncate=False)
            
            # 3. Guardado del Historial de Resultados (Pilar 3: Alertas)
            if is_db:
                try:
                    df_final.write.format("delta").mode("append").saveAsTable("bci_kqi_apolo.kqi_resultados")
                    logger.info("✅ Resultados históricos (Pilar 3) guardados en Delta Table bci_kqi_apolo.kqi_resultados")
                except Exception as e:
                    logger.error(f"Error escribiendo kqi_resultados en Delta: {e}")
            else:
                try:
                    resultados_json = [json.loads(row) for row in df_final.toJSON().collect()]
                    with open("mock_kqi_resultados.json", "w", encoding="utf-8") as f:
                        json.dump(resultados_json, f, indent=4, ensure_ascii=False)
                    logger.info("✅ Resultados históricos (Pilar 3) guardados en mock_kqi_resultados.json")
                except Exception as e:
                    logger.error(f"Error escribiendo mock_kqi_resultados: {e}")
                
            umbrales = config.get("parametros", {}).get("umbrales", {})
            umbral_gold = umbrales.get("aprobacion_gold", 0.80)
            umbral_hitl = umbrales.get("revision_hitl", 0.70)
            
            # 4. Promoción a Capa Oro (Gold)
            df_gold = df_final.filter(col("score_global") >= umbral_gold)
            if is_db:
                try:
                    df_gold.write.format("delta").mode("append").saveAsTable("bci_kqi_apolo.gold_aprobados")
                    logger.info(f"🏅 Capa Oro (Gold) actualizada en Delta Table bci_kqi_apolo.gold_aprobados (>= {umbral_gold})")
                except Exception as e:
                    logger.error(f"Error escribiendo gold_aprobados en Delta: {e}")
            else:
                try:
                    gold_json = [json.loads(row) for row in df_gold.toJSON().collect()]
                    with open("mock_gold_data.json", "w", encoding="utf-8") as f:
                        json.dump(gold_json, f, indent=4, ensure_ascii=False)
                    logger.info(f"🏅 Capa Oro (Gold) actualizada con {len(gold_json)} documentos válidos (>= {umbral_gold})")
                except Exception as e:
                    logger.error(f"Error escribiendo mock_gold_data: {e}")
                
            # 5. Envío a HITL (Human-in-the-loop)
            df_hitl = df_final.filter((col("score_global") >= umbral_hitl) & (col("score_global") < umbral_gold))
            if is_db:
                try:
                    df_hitl.write.format("delta").mode("append").saveAsTable("bci_kqi_apolo.hitl_revision")
                    logger.info(f"⚠️ Envío a HITL guardado en Delta Table bci_kqi_apolo.hitl_revision (entre {umbral_hitl} y {umbral_gold})")
                except Exception as e:
                    logger.error(f"Error escribiendo hitl_revision en Delta: {e}")
            else:
                try:
                    hitl_json = [json.loads(row) for row in df_hitl.toJSON().collect()]
                    if len(hitl_json) > 0:
                        with open("mock_hitl_revision.json", "w", encoding="utf-8") as f:
                            json.dump(hitl_json, f, indent=4, ensure_ascii=False)
                        logger.info(f"⚠️  {len(hitl_json)} documentos enviados a HITL (entre {umbral_hitl} y {umbral_gold}) en mock_hitl_revision.json")
                except Exception as e:
                    logger.error(f"Error escribiendo mock_hitl_revision: {e}")
                
            if is_db:
                logger.info("Procesamiento finalizado exitosamente en Databricks.")
            else:
                logger.info(f"Procesamiento finalizado. Total: {len(resultados_json)} | Gold: {len(gold_json)} | HITL: {len(hitl_json)}")
        else:
            logger.info("Ejecución simulada finalizada (Sin DataFrame real).")

if __name__ == "__main__":
    import sys
    doc_arg = sys.argv[1] if len(sys.argv) > 1 else None
    motor = SmartRouter()
    motor.ejecutar(doc_arg)
