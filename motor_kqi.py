import argparse
import sys
import os
import json
import uuid
import logging
import re
import math
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


# =============================================================================
# ARQUITECTURA PLUG-AND-PLAY DE EVALUADORES KQI
# Contratos estándar (Dataclass) + Factory + Registro de Plugins
# =============================================================================
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
import time as _time


@dataclass
class ContextoEvaluacion:
    """Contrato de ENTRADA estándar para todo evaluador KQI."""
    texto: str                              # Documento completo (nunca truncado)
    nombre_variable: str                    # Qué se está evaluando (ej. "Coherencia")
    params: Dict[str, Any] = field(default_factory=dict)  # Calibración específica del evaluador
    metadata_parse: Optional[str] = None    # Estructura VARIANT del ai_parse_document (JSON string)
    api_key: Optional[str] = None           # Solo para evaluadores LLM
    modelo_llm: Optional[str] = None        # Solo para evaluadores LLM
    variables_llm_grupo: Optional[List[str]] = None  # Variables a evaluar en batch por el LLM


@dataclass
class ResultadoEvaluacion:
    """Contrato de SALIDA estándar para todo evaluador KQI."""
    score: float                            # 0.0-1.0, polaridad ya resuelta (1.0 = máxima calidad)
    confianza: float = 1.0                  # 0.0-1.0, qué tan seguro está el evaluador
    metodo_usado: str = ""                  # Identificador del plugin (ej. "regex_ratio", "llm_gemini")
    latencia_ms: int = 0                    # Cuánto tardó la evaluación
    metadata: Dict[str, Any] = field(default_factory=dict)  # Libre: tokens, costo, matches, etc.
    comentarios: Optional[str] = None       # Hallazgos legibles (opcional)


# =============================================================================
# EVALUADORES (Plugins) — Cada uno recibe ContextoEvaluacion, retorna ResultadoEvaluacion
# =============================================================================

def plugin_regex_ratio(ctx: ContextoEvaluacion) -> ResultadoEvaluacion:
    """Mide proporción de caracteres que matchean un patrón regex."""
    t0 = _time.time()
    texto = ctx.texto or ""
    if not texto:
        return ResultadoEvaluacion(score=0.0, metodo_usado="regex_ratio", confianza=1.0)
    patron = ctx.params.get("patron", r"[^\w\s.,;:()\-\/]{2,}")
    matches = re.findall(patron, texto)
    total_chars = len(texto)
    chars_match = sum(len(m) for m in matches) if matches else 0
    ratio = chars_match / total_chars if total_chars > 0 else 0.0
    polaridad = ctx.params.get("polaridad", "negativa")
    score = (1.0 - ratio) if polaridad == "negativa" else ratio
    latencia = int((_time.time() - t0) * 1000)
    return ResultadoEvaluacion(
        score=round(score, 4),
        confianza=1.0,
        metodo_usado="regex_ratio",
        latencia_ms=latencia,
        metadata={"matches_count": len(matches), "chars_match": chars_match, "total_chars": total_chars}
    )


def plugin_element_count_ratio(ctx: ContextoEvaluacion) -> ResultadoEvaluacion:
    """Ratio de elementos de un tipo (ej. figure) vs total — usa metadata del parse."""
    t0 = _time.time()
    if not ctx.metadata_parse:
        return ResultadoEvaluacion(score=1.0, metodo_usado="element_count_ratio", confianza=0.5,
                                   comentarios="Sin metadata de parse disponible")
    try:
        parsed = json.loads(ctx.metadata_parse) if isinstance(ctx.metadata_parse, str) else ctx.metadata_parse
        elements = parsed.get("document", {}).get("elements", [])
        if not elements:
            return ResultadoEvaluacion(score=1.0, metodo_usado="element_count_ratio", confianza=0.7)
        tipo = ctx.params.get("tipo_elemento", "figure")
        count_tipo = sum(1 for e in elements if e.get("type") == tipo)
        total = len(elements)
        ratio = count_tipo / total if total > 0 else 0.0
        polaridad = ctx.params.get("polaridad", "negativa")
        score = (1.0 - ratio) if polaridad == "negativa" else ratio
        latencia = int((_time.time() - t0) * 1000)
        return ResultadoEvaluacion(
            score=round(score, 4), confianza=1.0, metodo_usado="element_count_ratio",
            latencia_ms=latencia, metadata={"count_tipo": count_tipo, "total_elements": total}
        )
    except Exception as e:
        return ResultadoEvaluacion(score=1.0, metodo_usado="element_count_ratio", confianza=0.3,
                                   comentarios=f"Error parsing metadata: {e}")


def plugin_shannon_entropy(ctx: ContextoEvaluacion) -> ResultadoEvaluacion:
    """Entropía de Shannon normalizada — mide desorden a nivel de caracteres."""
    t0 = _time.time()
    texto = ctx.texto or ""
    if len(texto) < 10:
        return ResultadoEvaluacion(score=1.0, metodo_usado="shannon_entropy", confianza=0.5)
    freq = {}
    for ch in texto:
        freq[ch] = freq.get(ch, 0) + 1
    total = len(texto)
    entropy = -sum((count / total) * math.log2(count / total) for count in freq.values())
    umbral_max = float(ctx.params.get("umbral_max", 6.5))
    normalized = min(entropy / umbral_max, 1.0)
    polaridad = ctx.params.get("polaridad", "negativa")
    score = (1.0 - normalized) if polaridad == "negativa" else normalized
    latencia = int((_time.time() - t0) * 1000)
    return ResultadoEvaluacion(
        score=round(score, 4), confianza=1.0, metodo_usado="shannon_entropy",
        latencia_ms=latencia, metadata={"entropy_raw": round(entropy, 4), "umbral_max": umbral_max}
    )


def plugin_compression_ratio(ctx: ContextoEvaluacion) -> ResultadoEvaluacion:
    """Ratio de compresión zlib — mide redundancia de contenido."""
    import zlib
    t0 = _time.time()
    texto = ctx.texto or ""
    if len(texto) < 50:
        return ResultadoEvaluacion(score=1.0, metodo_usado="compression_ratio", confianza=0.5)
    texto_bytes = texto.encode("utf-8")
    compressed = zlib.compress(texto_bytes, level=6)
    ratio = len(compressed) / len(texto_bytes)  # 0.0-1.0; bajo = muy repetitivo
    # Normalizar: ratio < 0.1 = extremadamente redundante, ratio > 0.6 = poco redundante
    umbral_min = float(ctx.params.get("umbral_min", 0.15))
    umbral_max = float(ctx.params.get("umbral_max", 0.60))
    score = max(0.0, min(1.0, (ratio - umbral_min) / (umbral_max - umbral_min)))
    polaridad = ctx.params.get("polaridad", "positiva")
    if polaridad == "negativa":
        score = 1.0 - score
    latencia = int((_time.time() - t0) * 1000)
    return ResultadoEvaluacion(
        score=round(score, 4), confianza=1.0, metodo_usado="compression_ratio",
        latencia_ms=latencia, metadata={"ratio_raw": round(ratio, 4), "original_bytes": len(texto_bytes), "compressed_bytes": len(compressed)}
    )


def plugin_largo_texto(ctx: ContextoEvaluacion) -> ResultadoEvaluacion:
    """Evalúa si el texto cumple un largo mínimo esperado."""
    t0 = _time.time()
    texto = ctx.texto or ""
    min_chars = int(ctx.params.get("min_chars", 100))
    ratio = min(len(texto) / min_chars, 1.0) if min_chars > 0 else 1.0
    polaridad = ctx.params.get("polaridad", "positiva")
    score = ratio if polaridad == "positiva" else (1.0 - ratio)
    latencia = int((_time.time() - t0) * 1000)
    return ResultadoEvaluacion(
        score=round(score, 4), confianza=1.0, metodo_usado="largo_texto",
        latencia_ms=latencia, metadata={"chars": len(texto), "min_chars": min_chars}
    )


def plugin_llm_gemini(ctx: ContextoEvaluacion) -> ResultadoEvaluacion:
    """
    Evaluador LLM (Gemini) — evalúa múltiples variables semánticas en una sola llamada.
    Retorna scores por variable en metadata['scores_por_variable'].
    """
    t0 = _time.time()
    api_key = ctx.api_key or os.environ.get("GOOGLE_API_KEY")
    modelo_llm = ctx.modelo_llm or "gemini-2.5-flash"
    alias_modelos = {
        "gemini-1.5-pro": "models/gemini-2.5-flash",
        "gemini-1.5-flash": "models/gemini-2.5-flash",
        "gemini-2.5-flash": "models/gemini-2.5-flash",
        "gemini-flash-latest": "models/gemini-flash-latest",
        "gemini-pro-latest": "models/gemini-pro-latest"
    }
    modelo_llm = alias_modelos.get(modelo_llm, modelo_llm)

    variables = ctx.variables_llm_grupo or [ctx.nombre_variable]
    variables_str = "', '".join(variables)

    if not api_key:
        mock = {v: 0.5 for v in variables}
        return ResultadoEvaluacion(
            score=0.5, confianza=0.0, metodo_usado="llm_gemini_mock",
            latencia_ms=0, metadata={"scores_por_variable": mock, "mock": True},
            comentarios="API Key no encontrada — resultado simulado"
        )

    try:
        import google.generativeai as genai

        texto_completo = ctx.texto or ""
        instruccion_sistema = (
            "Tu objetivo es evaluar la Calidad de Datos de este documento legal "
            "de forma ESTRICTAMENTE INTERNA. Busca contradicciones lógicas, fechas o montos que no cuadren "
            "dentro del mismo texto. NO utilices conocimiento externo, NO inventes reglas y NO lo compares "
            "con otras normativas. Analiza el documento como un ente aislado y autocontenido."
        )
        prompt = (
            f"{instruccion_sistema}\n\n"
            f"Devuelve exclusivamente un JSON válido con las claves '{variables_str}' entre 0.0 y 1.0 "
            "(donde 1.0 = máxima calidad / sin problemas, 0.0 = peor calidad). "
            "Puedes incluir una clave adicional 'Comentarios' con hallazgos breves y concretos.\n\n"
            "Documento completo a evaluar (sin truncar ni resumir):\n"
            f"{texto_completo}"
        )

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name=modelo_llm)
        response = model.generate_content(prompt)

        usage_metadata = getattr(response, "usage_metadata", None)
        tokens_in = getattr(usage_metadata, "prompt_token_count", 0) or 0
        tokens_out = getattr(usage_metadata, "candidates_token_count", 0) or 0
        latencia = int((_time.time() - t0) * 1000)

        # Parsear respuesta
        respuesta_raw = response.text
        cleaned = respuesta_raw.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]

        scores_parsed = json.loads(cleaned.strip())
        comentarios = scores_parsed.pop("Comentarios", None)

        # Calcular score promedio de las variables LLM
        scores_validos = [float(scores_parsed.get(v, 0.5)) for v in variables]
        score_promedio = sum(scores_validos) / len(scores_validos) if scores_validos else 0.5

        costo_usd = (tokens_in / 1_000_000 * 0.075) + (tokens_out / 1_000_000 * 0.30)

        return ResultadoEvaluacion(
            score=round(score_promedio, 4),
            confianza=0.85,
            metodo_usado="llm_gemini",
            latencia_ms=latencia,
            metadata={
                "scores_por_variable": {v: float(scores_parsed.get(v, 0.5)) for v in variables},
                "respuesta_llm": respuesta_raw,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "costo_usd": round(costo_usd, 6),
                "modelo_llm": modelo_llm
            },
            comentarios=str(comentarios) if comentarios else None
        )
    except Exception as e:
        latencia = int((_time.time() - t0) * 1000)
        return ResultadoEvaluacion(
            score=0.5, confianza=0.1, metodo_usado="llm_gemini",
            latencia_ms=latencia, metadata={"error": str(e)},
            comentarios=f"Error en llamada LLM: {e}"
        )


# =============================================================================
# FACTORY + REGISTRO GLOBAL DE PLUGINS
# Para agregar un evaluador nuevo: definir la función y registrarla aquí.
# =============================================================================

REGISTRO_EVALUADORES = {
    # Deterministas
    "regex_ratio": plugin_regex_ratio,
    "element_count_ratio": plugin_element_count_ratio,
    "shannon_entropy": plugin_shannon_entropy,
    "compression_ratio": plugin_compression_ratio,
    "largo_texto": plugin_largo_texto,
    # LLM
    "llm_gemini": plugin_llm_gemini,
}


class EvaluadorFactory:
    """Factory que instancia el evaluador correcto según configuración."""

    @staticmethod
    def evaluar(metodo: str, contexto: ContextoEvaluacion) -> ResultadoEvaluacion:
        """Ejecuta el evaluador registrado con el método dado."""
        plugin_fn = REGISTRO_EVALUADORES.get(metodo)
        if plugin_fn is None:
            logger.warning(f"Evaluador '{metodo}' no registrado. Retornando score neutro.")
            return ResultadoEvaluacion(
                score=0.5, confianza=0.0, metodo_usado=metodo,
                comentarios=f"Plugin '{metodo}' no encontrado en REGISTRO_EVALUADORES"
            )
        return plugin_fn(contexto)

    @staticmethod
    def listar_disponibles() -> List[str]:
        """Retorna los métodos de evaluación disponibles."""
        return list(REGISTRO_EVALUADORES.keys())


# =============================================================================
# RESOLVER CONFIGURACIÓN JERÁRQUICA → Plan de Evaluación
# =============================================================================

@dataclass
class PlanVariable:
    """Describe cómo evaluar UNA variable KQI."""
    nombre: str
    peso_efectivo: float
    metodo: str                    # Clave en REGISTRO_EVALUADORES
    params: Dict[str, Any] = field(default_factory=dict)
    categoria_padre: Optional[str] = None


def resolver_plan_evaluacion(pesos_kqi: dict) -> tuple:
    """
    Interpreta la estructura jerárquica de pesos_kqi y genera un plan de evaluación.
    Soporta formato plano (legacy) y jerárquico (nuevo).

    Retorna:
      plan_llm: list[PlanVariable] — variables a evaluar con LLM (batch)
      plan_det: list[PlanVariable] — variables a evaluar con plugins deterministas
      peso_total: float
    """
    plan_llm = []
    plan_det = []
    peso_total = 0.0

    for nombre, config in pesos_kqi.items():
        # Formato legacy: valor numérico directo → va al LLM
        if isinstance(config, (int, float)):
            plan_llm.append(PlanVariable(nombre=nombre, peso_efectivo=float(config), metodo="llm_gemini"))
            peso_total += float(config)
            continue

        peso_padre = float(config.get("peso", 0.0))
        peso_total += peso_padre
        fuente = config.get("fuente", "llm")
        metodo = config.get("metodo", "llm_gemini" if fuente == "llm" else "regex_ratio")
        params = dict(config.get("params", {}))
        if "polaridad" in config:
            params["polaridad"] = config["polaridad"]

        if fuente == "llm":
            plan_llm.append(PlanVariable(nombre=nombre, peso_efectivo=peso_padre, metodo=metodo, params=params))
        elif fuente == "determinista":
            plan_det.append(PlanVariable(nombre=nombre, peso_efectivo=peso_padre, metodo=metodo, params=params))
        elif fuente == "compuesta":
            subelementos = config.get("subelementos", {})
            total_sub_pesos = sum(float(s.get("peso", 1.0)) for s in subelementos.values())
            for sub_nombre, sub_config in subelementos.items():
                sub_peso_relativo = float(sub_config.get("peso", 1.0))
                peso_efectivo = peso_padre * (sub_peso_relativo / total_sub_pesos) if total_sub_pesos > 0 else 0.0
                sub_fuente = sub_config.get("fuente", "determinista")
                sub_metodo = sub_config.get("metodo", "llm_gemini" if sub_fuente == "llm" else "regex_ratio")
                sub_params = dict(sub_config.get("params", {}))
                if "polaridad" in sub_config:
                    sub_params["polaridad"] = sub_config["polaridad"]

                plan_var = PlanVariable(
                    nombre=sub_nombre, peso_efectivo=peso_efectivo,
                    metodo=sub_metodo, params=sub_params, categoria_padre=nombre
                )
                if sub_fuente == "llm":
                    plan_llm.append(plan_var)
                else:
                    plan_det.append(plan_var)

    return plan_llm, plan_det, peso_total


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

    def generar_id_ejecucion(self):
        """Genera un identificador único por corrida, priorizando contexto de Job si existe."""
        for env_key in [
            "DATABRICKS_JOB_RUN_ID",
            "DB_JOB_RUN_ID",
            "JOB_RUN_ID",
            "DATABRICKS_RUN_ID",
            "DATABRICKS_TASK_RUN_ID"
        ]:
            env_value = os.environ.get(env_key)
            if env_value:
                return str(env_value)
        return str(uuid.uuid4())

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

    def obtener_widget_texto(self, widget_name):
        """Obtiene un widget de Databricks si existe; retorna None si no está disponible."""
        if "DATABRICKS_RUNTIME_VERSION" not in os.environ or self.spark is None:
            return None
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            valor = dbutils.widgets.get(widget_name).strip()
            return valor or None
        except Exception:
            return None

    def resolver_id_documento_target(self, id_documento_target=None):
        """Resuelve el id del documento objetivo desde argumento, widget o variable de entorno."""
        if id_documento_target is not None and str(id_documento_target).strip() != "":
            return str(id_documento_target).strip()

        widget_value = self.obtener_widget_texto("id_documento_target")
        if widget_value:
            return widget_value

        env_value = os.environ.get("ID_DOCUMENTO_TARGET")
        if env_value and env_value.strip() != "":
            return env_value.strip()

        return None

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
                    val_lit = valor  # String

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
        Función que llama a la API de Gemini SOLO para variables con fuente='llm'.
        Las variables deterministas se evalúan por separado sin consumir tokens.
        """
        api_key = parametros.get("api_key") or os.environ.get("GOOGLE_API_KEY")

        modelo_llm = parametros.get("modelo_llm", "gemini-2.5-flash")
        alias_modelos = {
            "gemini-1.5-pro": "models/gemini-2.5-flash",
            "gemini-1.5-flash": "models/gemini-2.5-flash",
            "gemini-2.5-flash": "models/gemini-2.5-flash",
            "gemini-flash-latest": "models/gemini-flash-latest",
            "gemini-pro-latest": "models/gemini-pro-latest"
        }
        modelo_llm = alias_modelos.get(modelo_llm, modelo_llm)

        # Solo enviar a Gemini las variables LLM (no las deterministas)
        variables_llm = parametros.get("_variables_llm_nombres", [])
        if not variables_llm:
            # Fallback legacy: todas las claves de pesos_kqi
            variables_llm = list(parametros.get("pesos_kqi", {}).keys())
        if not variables_llm:
            variables_llm = ["Coherencia", "Ambiguedad", "Entropia"]

        variables_str = "', '".join(variables_llm)

        if not api_key:
            logger.warning("[MOCK LLM] API Key no encontrada. Simulando respuesta de Gemini.")
            mock_response = {v: 0.5 for v in variables_llm}
            mock_response["Comentarios"] = "Texto simulado evaluado exitosamente."
            return json.dumps(mock_response)

        try:
            import google.generativeai as genai

            texto_completo = "" if texto is None else str(texto)
            instruccion_sistema = (
                "Tu objetivo es evaluar la Calidad de Datos de este documento legal "
                "de forma ESTRICTAMENTE INTERNA. Busca contradicciones lógicas, fechas o montos que no cuadren "
                "dentro del mismo texto. NO utilices conocimiento externo, NO inventes reglas y NO lo compares "
                "con otras normativas. Analiza el documento como un ente aislado y autocontenido."
            )
            prompt = (
                f"{instruccion_sistema}\n\n"
                f"Devuelve exclusivamente un JSON válido con las claves '{variables_str}' entre 0.0 y 1.0 "
                "(donde 1.0 = máxima calidad / sin problemas, 0.0 = peor calidad). "
                "Puedes incluir una clave adicional 'Comentarios' con hallazgos breves y concretos.\n\n"
                "Documento completo a evaluar (sin truncar ni resumir):\n"
                f"{texto_completo}"
            )

            genai.configure(api_key=api_key)
            model = genai.GenerativeModel(model_name=modelo_llm)
            response = model.generate_content(prompt)

            usage_metadata = getattr(response, "usage_metadata", None)
            tokens_in = getattr(usage_metadata, "prompt_token_count", 0) or 0
            tokens_out = getattr(usage_metadata, "candidates_token_count", 0) or 0

            resultado = {
                "respuesta_llm": response.text,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "modelo_llm": modelo_llm
            }
            return json.dumps(resultado)
        except Exception as e:
            logger.error(f"Error al llamar a Gemini: {e}")
            return None

    @staticmethod
    def detectar_pii(texto, regex_dict):
        import re
        if not texto:
            return "{}"
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
            if total_peso == 0:
                return 0.0

            for k, peso in pesos.items():
                val = float(data.get(k, 0.0))
                # Variables donde mayor valor = peor calidad
                if k in ["ambiguedad", "entropia", "palabras_ilegibles", "ratio_imagenes"]:
                    val_positivo = 1.0 - val
                else:
                    val_positivo = val  # Variables donde mayor valor = mejor calidad (ej. coherencia)

                score_acumulado += val_positivo * float(peso)

            score = score_acumulado / total_peso
            return float(round(score, 4))
        except Exception:
            return 0.0

    def procesar_no_estructurado(self, df, parametros):
        """
        Ruta No Estructurada: Arquitectura Plug-and-Play.
        Usa EvaluadorFactory + contratos estándar (ContextoEvaluacion / ResultadoEvaluacion).
        El motor SOLO orquesta y consolida — no conoce los detalles internos de cada evaluador.
        """
        logger.info("Iniciando procesamiento No Estructurado (Plugin Architecture).")
        logger.info("Modo on-demand 1-a-1: coherencia estrictamente interna, SLA < 60s.")

        if self.spark is None or df is None:
            logger.warning("[MOCK] Spark no disponible o DataFrame vacío.")
            return None

        # Generar plan de evaluación desde configuración jerárquica
        pesos_kqi = parametros.get("pesos_kqi", {})
        plan_llm, plan_det, peso_total = resolver_plan_evaluacion(pesos_kqi)

        logger.info(f"Plan LLM ({len(plan_llm)}): {[p.nombre for p in plan_llm]}")
        logger.info(f"Plan Determinista ({len(plan_det)}): {[p.nombre for p in plan_det]}")
        logger.info(f"Peso total: {peso_total}")
        logger.info(f"Evaluadores disponibles: {EvaluadorFactory.listar_disponibles()}")

        # Obtener API Key en el driver
        api_key = self.obtener_api_key()
        modelo_llm = parametros.get("modelo_llm", "gemini-2.5-flash")
        variables_llm_nombres = [p.nombre for p in plan_llm]

        # --- UDF LLM: invoca plugin_llm_gemini via Factory ---
        def evaluar_llm_fn(texto):
            ctx = ContextoEvaluacion(
                texto=texto or "",
                nombre_variable="_batch_llm",
                params={},
                api_key=api_key,
                modelo_llm=modelo_llm,
                variables_llm_grupo=variables_llm_nombres
            )
            resultado = EvaluadorFactory.evaluar("llm_gemini", ctx)
            return json.dumps({
                "score": resultado.score,
                "confianza": resultado.confianza,
                "metodo_usado": resultado.metodo_usado,
                "latencia_ms": resultado.latencia_ms,
                "metadata": resultado.metadata,
                "comentarios": resultado.comentarios
            })

        llm_udf = udf(evaluar_llm_fn, StringType())

        # --- UDF Determinista: invoca cada plugin via Factory ---
        plan_det_serializable = [
            {"nombre": p.nombre, "metodo": p.metodo, "params": p.params}
            for p in plan_det
        ]

        def evaluar_det_fn(texto, metadata_parse_str):
            """Evalua variables deterministas pasando metadata_parse del ai_parse_document."""
            resultados = {}
            for var_info in plan_det_serializable:
                ctx = ContextoEvaluacion(
                    texto=texto or "",
                    nombre_variable=var_info["nombre"],
                    params=var_info["params"],
                    metadata_parse=metadata_parse_str
                )
                resultado = EvaluadorFactory.evaluar(var_info["metodo"], ctx)
                resultados[var_info["nombre"]] = {
                    "score": resultado.score,
                    "confianza": resultado.confianza,
                    "metodo_usado": resultado.metodo_usado,
                    "latencia_ms": resultado.latencia_ms
                }
            return json.dumps(resultados)

        det_udf = udf(evaluar_det_fn, StringType())

        # --- UDF PII ---
        regex_dict = parametros.get("regex_pii", {})
        pii_udf = udf(lambda txt: SmartRouter.detectar_pii(txt, regex_dict), StringType())

        from pyspark.sql.functions import regexp_replace, get_json_object, when

        # Ejecutar evaluaciones
        df_temp = df.withColumn("tipo_procesamiento", lit("No Estructurado"))

        if plan_llm:
            df_temp = df_temp.withColumn("resultado_llm", llm_udf(col("texto_transcrito")))
        else:
            df_temp = df_temp.withColumn("resultado_llm", lit(None).cast(StringType()))

        if plan_det:
            df_temp = df_temp.withColumn("scores_deterministas", det_udf(col("texto_transcrito"), col("metadata_parse")))
        else:
            df_temp = df_temp.withColumn("scores_deterministas", lit("{}"))

        df_temp = df_temp.withColumn("hallazgos_pii", pii_udf(col("texto_transcrito")))

        # Extraer metadata del resultado LLM para persistencia
        df_temp = df_temp \
            .withColumn("detalle_error_llm",
                        get_json_object(col("resultado_llm"), "$.metadata.respuesta_llm")) \
            .withColumn("detalle_gemini", col("resultado_llm")) \
            .withColumn("tokens_in",
                        get_json_object(col("resultado_llm"), "$.metadata.tokens_in").cast("int")) \
            .withColumn("tokens_out",
                        get_json_object(col("resultado_llm"), "$.metadata.tokens_out").cast("int")) \
            .withColumn("costo_estimado_usd",
                        when(col("tokens_in").isNotNull(),
                             (col("tokens_in") / 1000000.0 * 0.075) + (col("tokens_out") / 1000000.0 * 0.30)
                        ).otherwise(lit(0.0)))

        # --- Consolidar score_global ponderando LLM + Determinista ---
        score_expr = lit(0.0)

        for plan_var in plan_llm:
            val_expr = get_json_object(
                col("resultado_llm"), f"$.metadata.scores_por_variable.{plan_var.nombre}"
            ).cast("double")
            val_expr = when(val_expr.isNull(), 0.5).otherwise(val_expr)
            score_expr = score_expr + (val_expr * lit(plan_var.peso_efectivo))

        for plan_var in plan_det:
            val_expr = get_json_object(
                col("scores_deterministas"), f"$.{plan_var.nombre}.score"
            ).cast("double")
            val_expr = when(val_expr.isNull(), 0.5).otherwise(val_expr)
            score_expr = score_expr + (val_expr * lit(plan_var.peso_efectivo))

        if peso_total > 0:
            score_expr = score_expr / lit(peso_total)

        df_resultado = df_temp.withColumn("score_global", score_expr) \
                              .withColumn("ruta_nodo_json_error", lit(None).cast(StringType()))

        # --- Cálculo explícito de scores para variables compuestas (padres) ---
        # El padre NO se evalúa independientemente; su score es el promedio ponderado
        # de sus subelementos directos. Se almacena en scores_compuestos para trazabilidad.
        padres_con_hijos = {}
        for plan_var in (plan_llm + plan_det):
            if plan_var.categoria_padre:
                padres_con_hijos.setdefault(plan_var.categoria_padre, []).append(plan_var)

        if padres_con_hijos:
            from pyspark.sql.functions import struct, to_json, coalesce

            scores_padres_expr = lit("{")
            first_padre = True
            for padre, hijos in padres_con_hijos.items():
                peso_padre_total = sum(h.peso_efectivo for h in hijos)
                # score_padre = sum(hijo.score * hijo.peso_efectivo) / sum(hijo.peso_efectivo)
                padre_score_expr = lit(0.0)
                for hijo in hijos:
                    if hijo in plan_llm:
                        val = get_json_object(col("resultado_llm"), f"$.metadata.scores_por_variable.{hijo.nombre}").cast("double")
                    else:
                        val = get_json_object(col("scores_deterministas"), f"$.{hijo.nombre}.score").cast("double")
                    val = when(val.isNull(), lit(0.5)).otherwise(val)
                    padre_score_expr = padre_score_expr + (val * lit(hijo.peso_efectivo))

                if peso_padre_total > 0:
                    padre_score_expr = padre_score_expr / lit(peso_padre_total)

                # Construir JSON string para cada padre
                sep = lit("") if first_padre else lit(", ")
                scores_padres_expr = scores_padres_expr + sep + lit(f'"{padre}": ') + padre_score_expr.cast("string")
                first_padre = False

            scores_padres_expr = scores_padres_expr + lit("}")

            # Usar UDF simple para generar JSON válido de scores compuestos
            all_plan_vars = plan_llm + plan_det
            padres_info = {padre: [(h.nombre, h.peso_efectivo, h in plan_llm) for h in hijos] 
                         for padre, hijos in padres_con_hijos.items()}

            def calcular_scores_compuestos(resultado_llm_json, scores_det_json):
                """Calcula score agregado por cada variable compuesta (padre)."""
                scores_llm = {}
                scores_det = {}
                if resultado_llm_json:
                    try:
                        llm_data = json.loads(resultado_llm_json)
                        scores_llm = llm_data.get("metadata", {}).get("scores_por_variable", {})
                    except Exception:
                        pass
                if scores_det_json:
                    try:
                        scores_det = json.loads(scores_det_json)
                    except Exception:
                        pass

                result = {}
                for padre, hijos_info in padres_info.items():
                    peso_total_padre = sum(h[1] for h in hijos_info)
                    score_sum = 0.0
                    for nombre_hijo, peso_hijo, es_llm in hijos_info:
                        if es_llm:
                            score_hijo = float(scores_llm.get(nombre_hijo, 0.5))
                        else:
                            det_data = scores_det.get(nombre_hijo, {})
                            score_hijo = float(det_data.get("score", 0.5)) if isinstance(det_data, dict) else 0.5
                        score_sum += score_hijo * peso_hijo
                    result[padre] = round(score_sum / peso_total_padre, 6) if peso_total_padre > 0 else 0.0
                return json.dumps(result)

            compuestos_udf = udf(calcular_scores_compuestos, StringType())
            df_resultado = df_resultado.withColumn(
                "scores_compuestos",
                compuestos_udf(col("resultado_llm"), col("scores_deterministas"))
            )
        else:
            df_resultado = df_resultado.withColumn("scores_compuestos", lit("{}"))

        return df_resultado

    def escribir_alerta_lote(self, promedio_lote, umbral_alerta, nombre_documento):
        """Persistencia liviana de alertas para lote crítico."""
        import time

        if promedio_lote is None or promedio_lote >= umbral_alerta:
            if promedio_lote is not None:
                logger.info(f"Lote Saludable: Confianza promedio {promedio_lote:.2f}.")
            return

        logger.warning(
            f"¡ALERTA DE GOBIERNO! El lote analizado tiene una confianza promedio de {promedio_lote:.2f}, "
            f"menor al umbral del {umbral_alerta * 100}% ({umbral_alerta})."
        )

        if not os.path.exists("alertas"):
            os.makedirs("alertas")

        alerta_data = {
            "proyecto": nombre_documento,
            "timestamp": time.time(),
            "score_promedio": promedio_lote,
            "umbral": umbral_alerta
        }
        with open(f"alertas/alerta_lote_{int(time.time())}.json", "w", encoding="utf-8") as fa:
            json.dump(alerta_data, fa, ensure_ascii=False)

    def enrutar_resultados(self, id_ejecucion_actual, config, nombre_documento, is_db, df_resultados_local=None):
        """Lee resultados ya persistidos y enruta Gold/HITL sin re-ejecutar la UDF de IA."""
        from pyspark.sql.functions import avg

        if self.spark is None:
            logger.warning("[MOCK] Spark no disponible. No es posible enrutar resultados.")
            return None

        umbrales = config.get("parametros", {}).get("umbrales", {})
        umbral_gold = umbrales.get("aprobacion_gold", 0.80)
        umbral_hitl = umbrales.get("revision_hitl", 0.70)
        umbral_alerta = umbrales.get("alerta_lote_critico", 0.85)

        if is_db:
            df_materializado = self.spark.read.table("bci_kqi_apolo.kqi_resultados") \
                .filter(col("id_ejecucion") == lit(id_ejecucion_actual))
        else:
            try:
                df_materializado = self.spark.read.option("multiline", "true").json("mock_kqi_resultados.json") \
                    .filter(col("id_ejecucion") == lit(id_ejecucion_actual))
            except Exception:
                if df_resultados_local is None:
                    logger.error("No se pudieron releer los resultados mock materializados.")
                    return None
                df_materializado = df_resultados_local.filter(col("id_ejecucion") == lit(id_ejecucion_actual))

        if not df_materializado.take(1):
            logger.warning(f"No se encontraron resultados materializados para id_ejecucion={id_ejecucion_actual}")
            return {"total": 0, "gold": 0, "hitl": 0}

        promedio_lote = None
        try:
            promedio_lote = df_materializado.select(avg("score_global")).collect()[0][0]
        except Exception as e:
            logger.error(f"No se pudo calcular el promedio del lote para alertas: {e}")

        self.escribir_alerta_lote(promedio_lote, umbral_alerta, nombre_documento)

        # --- Clasificación Medallón: Gold / HITL / Rechazados ---
        df_gold = df_materializado.filter(col("score_global") >= umbral_gold)
        df_hitl = df_materializado.filter((col("score_global") >= umbral_hitl) & (col("score_global") < umbral_gold))
        df_rechazados = df_materializado.filter(col("score_global") < umbral_hitl)

        total_registros = df_materializado.count()
        total_gold = df_gold.count()
        total_hitl = df_hitl.count()
        total_rechazados = df_rechazados.count()

        if is_db:
            # Gold: documentos aprobados automáticamente
            if total_gold > 0:
                try:
                    df_gold.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("bci_kqi_apolo.gold_aprobados")
                    logger.info(f"🏅 Gold: {total_gold} documentos promovidos (score >= {umbral_gold})")
                except Exception as e:
                    logger.error(f"Error escribiendo gold_aprobados: {e}")

            # HITL: documentos para revisión humana
            if total_hitl > 0:
                try:
                    df_hitl.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("bci_kqi_apolo.hitl_revision")
                    logger.info(f"⚠️ HITL: {total_hitl} documentos a revisión humana ({umbral_hitl} <= score < {umbral_gold})")
                except Exception as e:
                    logger.error(f"Error escribiendo hitl_revision: {e}")

            # Rechazados: documentos que no cumplen el mínimo
            if total_rechazados > 0:
                try:
                    df_rechazados.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("bci_kqi_apolo.rechazados")
                    logger.info(f"❌ Rechazados: {total_rechazados} documentos bajo umbral mínimo (score < {umbral_hitl})")
                except Exception as e:
                    logger.error(f"Error escribiendo rechazados: {e}")

            # Marcar documentos procesados en Silver (añadir columna estado_auditoria)
            try:
                ids_procesados = [row.id_documento for row in df_materializado.select("id_documento").distinct().collect()]
                if ids_procesados:
                    ids_sql = ",".join([f"'{id_doc}'" for id_doc in ids_procesados])
                    self.spark.sql(f"""
                        MERGE INTO bci_kqi_apolo.silver_transcripciones AS target
                        USING (SELECT explode(array({ids_sql})) AS id_documento) AS source
                        ON target.id_documento = source.id_documento
                        WHEN MATCHED THEN UPDATE SET target.estado_auditoria = 'procesado'
                    """)
                    logger.info(f"📋 Silver: {len(ids_procesados)} documentos marcados como 'procesado'")
            except Exception as e:
                logger.warning(f"No se pudo marcar Silver (columna estado_auditoria puede no existir aún): {e}")

        else:
            try:
                gold_json = [json.loads(row) for row in df_gold.toJSON().collect()]
                with open("mock_gold_data.json", "w", encoding="utf-8") as f:
                    json.dump(gold_json, f, indent=4, ensure_ascii=False)
                logger.info(f"🏅 Gold: {len(gold_json)} documentos (>= {umbral_gold})")
            except Exception as e:
                logger.error(f"Error escribiendo mock_gold_data: {e}")

            try:
                hitl_json = [json.loads(row) for row in df_hitl.toJSON().collect()]
                if hitl_json:
                    with open("mock_hitl_revision.json", "w", encoding="utf-8") as f:
                        json.dump(hitl_json, f, indent=4, ensure_ascii=False)
                    logger.info(f"⚠️ HITL: {len(hitl_json)} documentos a revisión")
            except Exception as e:
                logger.error(f"Error escribiendo mock_hitl_revision: {e}")

            try:
                rech_json = [json.loads(row) for row in df_rechazados.toJSON().collect()]
                if rech_json:
                    with open("mock_rechazados.json", "w", encoding="utf-8") as f:
                        json.dump(rech_json, f, indent=4, ensure_ascii=False)
                    logger.info(f"❌ Rechazados: {len(rech_json)} documentos bajo umbral")
            except Exception as e:
                logger.error(f"Error escribiendo mock_rechazados: {e}")

        return {
            "total": total_registros,
            "gold": total_gold,
            "hitl": total_hitl,
            "rechazados": total_rechazados,
            "promedio_lote": promedio_lote
        }

    def ingerir_y_aplanar(self, file_path="mock_data.json", is_db=False, id_documento_target=None):
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

            if id_documento_target:
                df_raw = df_raw.filter(col("id_documento") == lit(id_documento_target))
                logger.info(f"Filtro temprano aplicado para id_documento_target={id_documento_target}")

            # Supongamos que hay un arreglo anidado 'detalles'
            if "detalles" in df_raw.columns:
                df_flat = df_raw.withColumn("detalle_aplanado", explode_outer(col("detalles")))
                return df_flat
            return df_raw
        except Exception as e:
            logger.warning(f"No se pudo leer/aplanar {file_path if not is_db else 'bci_kqi_apolo.silver_transcripciones'}. Error: {e}")
            return None

    def ejecutar(self, nombre_documento=None, id_documento_target=None):
        """
        Orquestador principal
        """
        is_db = "DATABRICKS_RUNTIME_VERSION" in os.environ
        id_ejecucion = self.generar_id_ejecucion()
        id_documento_target = self.resolver_id_documento_target(id_documento_target)

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
                is_db = False  # Fallback a modo local

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
        logger.info(
            f"Auditoría para: '{nombre_documento}' | Ruta detectada: {tipo_proc} | "
            f"Entorno Databricks: {is_db} | id_ejecucion: {id_ejecucion} | "
            f"id_documento_target: {id_documento_target or 'ALL'}"
        )

        # 1. Lectura desde Capa Plata (Silver)
        df_silver = self.ingerir_y_aplanar(
            "mock_silver_data.json",
            is_db=is_db,
            id_documento_target=id_documento_target
        )
        if df_silver is None:
            logger.error("No se pudo leer la capa Plata.")
            return

        # Filtramos solo los documentos de este tipo
        df_silver_filtrado = df_silver.filter(col("nombre_documento") == nombre_documento)
        if not df_silver_filtrado.take(1):
            logger.warning(
                f"No se encontraron registros para nombre_documento='{nombre_documento}' "
                f"e id_documento_target='{id_documento_target}'."
            )
            return

        # 2. Evaluación KQI
        df_final = None
        if tipo_proc == "Semiestructurado":
            df_final = self.procesar_semiestructurado(df_silver_filtrado, config.get("parametros", {}).get("reglas", []))
        elif tipo_proc == "No Estructurado":
            df_final = self.procesar_no_estructurado(df_silver_filtrado, config.get("parametros", {}))

        if df_final is not None:
            df_final = df_final.withColumn("nombre_tipo_documento", lit(nombre_documento)) \
                               .withColumn("id_ejecucion", lit(id_ejecucion))

            # 3. Guardado del Historial de Resultados (única acción inmediata post-evaluación)
            if is_db:
                try:
                    df_final.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("bci_kqi_apolo.kqi_resultados")
                    logger.info("✅ Resultados históricos guardados en Delta Table bci_kqi_apolo.kqi_resultados")
                except Exception as e:
                    logger.error(f"Error escribiendo kqi_resultados en Delta: {e}")
                    return
            else:
                try:
                    resultados_json = [json.loads(row) for row in df_final.toJSON().collect()]
                    with open("mock_kqi_resultados.json", "w", encoding="utf-8") as f:
                        json.dump(resultados_json, f, indent=4, ensure_ascii=False)
                    logger.info("✅ Resultados históricos guardados en mock_kqi_resultados.json")
                except Exception as e:
                    logger.error(f"Error escribiendo mock_kqi_resultados: {e}")
                    return

            # 4. Enrutamiento posterior desde resultados ya materializados
            resumen = self.enrutar_resultados(
                id_ejecucion_actual=id_ejecucion,
                config=config,
                nombre_documento=nombre_documento,
                is_db=is_db,
                df_resultados_local=df_final if not is_db else None
            )

            if is_db:
                logger.info("Procesamiento finalizado exitosamente en Databricks.")
            elif resumen is not None:
                logger.info(
                    f"Procesamiento finalizado. Total: {resumen['total']} | "
                    f"Gold: {resumen['gold']} | HITL: {resumen['hitl']}"
                )
        else:
            logger.info("Ejecución simulada finalizada (Sin DataFrame real).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Motor KQI Apolo")
    parser.add_argument("nombre_documento", nargs="?", default=None)
    parser.add_argument("--id_documento_target", dest="id_documento_target", default=None)
    args = parser.parse_args()

    motor = SmartRouter()
    motor.ejecutar(args.nombre_documento, args.id_documento_target)
