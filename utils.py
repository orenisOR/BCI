import streamlit as st
import json
import os
import yaml

WAREHOUSE_ID = "0e6bd176c827b16a"
CATALOG = "workspace"
SCHEMA = "bci_kqi_apolo"


def inyectar_css_corporativo():
    """Inyecta el CSS para aplicar un diseño visual premium con estilo corporativo Bci."""
    st.markdown("""
    <style>
        /* Ocultar elementos nativos de Streamlit */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        [data-testid="stSidebarNav"] {display: none;} /* Oculta navegación nativa */

        /* Importar Tipografía Premium */
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');

        /* Aplicar fuente Outfit a todos los componentes */
        html, body, [class*="css"], .stApp, .stMarkdown, p, span, h1, h2, h3, h4, h5, h6 {
            font-family: 'Outfit', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif !important;
        }

        /* Estilo elegante para la barra lateral (Sidebar) */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #091C3E 0%, #030A18 100%) !important;
            border-right: 1px solid rgba(0, 229, 255, 0.1) !important;
            box-shadow: 4px 0 15px rgba(0, 0, 0, 0.3) !important;
        }

        [data-testid="stSidebar"] * {
            color: #E2E8F0 !important;
        }

        [data-testid="stSidebar"] a {
            border-radius: 8px !important;
            padding: 8px 12px !important;
            transition: all 0.2s ease !important;
        }

        [data-testid="stSidebar"] a:hover {
            background: rgba(0, 229, 255, 0.1) !important;
            transform: translateX(4px) !important;
        }

        /* Estilo de Tarjetas de Información (Bci Cards) */
        .bci-card {
            background: rgba(255, 255, 255, 0.02) !important;
            border: 1px solid rgba(0, 229, 255, 0.15) !important;
            border-radius: 12px !important;
            padding: 24px !important;
            margin-bottom: 20px !important;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1) !important;
        }

        .bci-card:hover {
            transform: translateY(-4px) scale(1.005) !important;
            border-color: #00E5FF !important;
            box-shadow: 0 10px 20px rgba(0, 229, 255, 0.1) !important;
            background: rgba(255, 255, 255, 0.04) !important;
        }

        /* Título con Gradiente de Alta Fidelidad */
        .bci-gradient-title {
            background: linear-gradient(90deg, #00E5FF 0%, #0072FF 100%) !important;
            -webkit-background-clip: text !important;
            -webkit-text-fill-color: transparent !important;
            font-weight: 800 !important;
            letter-spacing: -0.5px !important;
        }

        /* Botón de Acción Principal (Estilo Premium) */
        button[kind="primary"] {
            background: linear-gradient(135deg, #0052D4 0%, #4364F7 50%, #6FB1FC 100%) !important;
            border: none !important;
            color: white !important;
            font-weight: 600 !important;
            border-radius: 8px !important;
            padding: 10px 24px !important;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
            box-shadow: 0 4px 14px rgba(67, 100, 247, 0.4) !important;
        }

        button[kind="primary"]:hover {
            transform: translateY(-2px) scale(1.02) !important;
            box-shadow: 0 6px 20px rgba(67, 100, 247, 0.6) !important;
        }

        /* Botón de Acción Secundario / Otros Botones */
        button[kind="secondary"], button:not([kind="primary"]) {
            border-radius: 8px !important;
            transition: all 0.2s ease !important;
        }

        button[kind="secondary"]:hover, button:not([kind="primary"]):hover {
            border-color: #00E5FF !important;
            color: #00E5FF !important;
            background: rgba(0, 229, 255, 0.05) !important;
        }

        /* Inputs y Campos de Entrada Elegantes */
        .stTextInput input, .stSelectbox select, .stNumberInput input {
            border-radius: 8px !important;
            transition: all 0.3s ease !important;
        }

        .stTextInput input:focus, .stSelectbox select:focus, .stNumberInput input:focus {
            border-color: #00E5FF !important;
            box-shadow: 0 0 0 2px rgba(0, 229, 255, 0.2) !important;
        }
    </style>
    """, unsafe_allow_html=True)


def es_entorno_databricks():
    """Detecta si el script se está ejecutando dentro de Databricks o Databricks Apps."""
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or "DATABRICKS_APP_PORT" in os.environ
        or "DATABRICKS_APP_NAME" in os.environ
    )


def _parsear_parametros_json(raw_value):
    """Parsea JSON tolerando filas históricas con backslashes mal escapados."""
    if not raw_value:
        return {}

    if isinstance(raw_value, dict):
        return raw_value

    try:
        return json.loads(raw_value)
    except json.JSONDecodeError:
        json_normalizado = raw_value.replace("\\", "\\\\")
        return json.loads(json_normalizado)


def _escape_sql_literal(value):
    """Escapa texto para interpolarlo de forma segura en literales SQL."""
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace("'", "''")


def ejecutar_sql(statement, fetch=False):
    """Ejecuta una sentencia SQL usando la API Statement Execution del SDK."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
    import time

    w = WorkspaceClient()

    response = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        catalog=CATALOG,
        schema=SCHEMA,
        wait_timeout="50s"
    )

    while response.status and response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        response = w.statement_execution.get_statement(response.statement_id)

    if response.status and response.status.state == StatementState.FAILED:
        error_msg = response.status.error.message if response.status.error else "Error desconocido"
        raise Exception(f"SQL Error: {error_msg}")

    if fetch and response.result and response.result.data_array:
        return response.result.data_array

    return []


def obtener_usuario_actual():
    """Recupera el current_user() desde el entorno de Databricks."""
    try:
        from streamlit import context
        headers = context.headers
        if "X-Forwarded-Email" in headers:
            return headers["X-Forwarded-Email"]
        if "DATABRICKS_CURRENT_USER" in os.environ:
            return os.environ["DATABRICKS_CURRENT_USER"]
        raise Exception("No headers")
    except Exception:
        return "admin@bci.cl"


def es_administrador():
    """Valida si el usuario actual está en la lista de administradores."""
    usuario_actual = obtener_usuario_actual()
    try:
        with open("config_variables.json", "r", encoding="utf-8") as f:
            variables_config = json.load(f)
            admin_list = variables_config.get("administradores", [])
    except FileNotFoundError:
        admin_list = []
    return usuario_actual in admin_list


def generar_sidebar():
    """Genera el menú lateral personalizado."""
    inyectar_css_corporativo()
    usuario = obtener_usuario_actual()
    perfil = "Administrador" if es_administrador() else "Lectura"

    st.sidebar.markdown(f"\U0001f464 **Usuario:**\n`{usuario}`\n\n\U0001f6e1\ufe0f **Perfil:** `{perfil}`")
    st.sidebar.divider()
    st.sidebar.markdown("**Navegación**")
    st.sidebar.page_link("app.py", label="\U0001f4c2 Dashboard Inventario")
    st.sidebar.page_link("pages/1_Crear_Documento.py", label="\u2795 Nuevo Documento")


def cargar_configuracion_maestra():
    if es_entorno_databricks():
        try:
            ejecutar_sql("""
                CREATE TABLE IF NOT EXISTS kqi_configuracion (
                    id_config STRING NOT NULL,
                    nombre_tipo_documento STRING NOT NULL,
                    tipo_procesamiento STRING NOT NULL,
                    parametros_json STRING,
                    usuario_modificacion STRING,
                    fecha_modificacion TIMESTAMP
                ) USING DELTA
            """)

            rows = ejecutar_sql(
                "SELECT nombre_tipo_documento, tipo_procesamiento, parametros_json, fecha_modificacion FROM kqi_configuracion",
                fetch=True
            )
            maestra = {}
            for row in rows:
                nombre = row[0]
                tipo_proc = row[1]
                params = _parsear_parametros_json(row[2]) if row[2] else {}
                fecha = str(row[3]) if row[3] else ""
                maestra[nombre] = {
                    "tipo_procesamiento": tipo_proc,
                    "parametros": params,
                    "timestamp": fecha
                }
            return maestra
        except Exception as e:
            st.sidebar.warning(f"\u26a0\ufe0f Databricks SQL error (usando fallback mock local): {e}")

    try:
        with open("mock_config.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            if "tipo_procesamiento" in data and isinstance(data.get("tipo_procesamiento"), str):
                return {}
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def guardar_configuracion_maestra(maestra):
    if es_entorno_databricks():
        try:
            usuario = obtener_usuario_actual()
            import datetime
            import uuid

            ejecutar_sql("""
                CREATE TABLE IF NOT EXISTS kqi_configuracion (
                    id_config STRING NOT NULL,
                    nombre_tipo_documento STRING NOT NULL,
                    tipo_procesamiento STRING NOT NULL,
                    parametros_json STRING,
                    usuario_modificacion STRING,
                    fecha_modificacion TIMESTAMP
                ) USING DELTA
            """)

            for nombre, datos in maestra.items():
                tipo_proc = datos.get("tipo_procesamiento", "No Estructurado")
                params = json.dumps(datos.get("parametros", {}), ensure_ascii=False)
                timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                id_config = str(uuid.uuid4())

                nombre_sql = _escape_sql_literal(nombre)
                tipo_proc_sql = _escape_sql_literal(tipo_proc)
                params_sql = _escape_sql_literal(params)
                usuario_sql = _escape_sql_literal(usuario)
                id_config_sql = _escape_sql_literal(id_config)
                timestamp_sql = _escape_sql_literal(timestamp_str)

                ejecutar_sql(f"""
                    MERGE INTO kqi_configuracion AS target
                    USING (
                        SELECT
                            '{nombre_sql}' AS nombre_tipo_documento,
                            '{tipo_proc_sql}' AS tipo_procesamiento,
                            '{params_sql}' AS parametros_json,
                            '{usuario_sql}' AS usuario_modificacion,
                            CAST('{timestamp_sql}' AS TIMESTAMP) AS fecha_modificacion
                    ) AS source
                    ON target.nombre_tipo_documento = source.nombre_tipo_documento
                    WHEN MATCHED THEN
                        UPDATE SET
                            tipo_procesamiento = source.tipo_procesamiento,
                            parametros_json = source.parametros_json,
                            usuario_modificacion = source.usuario_modificacion,
                            fecha_modificacion = source.fecha_modificacion
                    WHEN NOT MATCHED THEN
                        INSERT (id_config, nombre_tipo_documento, tipo_procesamiento, parametros_json, usuario_modificacion, fecha_modificacion)
                        VALUES ('{id_config_sql}', source.nombre_tipo_documento, source.tipo_procesamiento, source.parametros_json, source.usuario_modificacion, source.fecha_modificacion)
                """)

            st.success("\u2705 Configuración guardada en Unity Catalog (bci_kqi_apolo.kqi_configuracion)")
            return
        except Exception as e:
            st.error(f"\u274c Error en Databricks: {e}. Guardando copia en local...")

    with open("mock_config.json", "w", encoding="utf-8") as f:
        json.dump(maestra, f, indent=4, ensure_ascii=False)


def ejecutar_auditoria_kqi(documento_nombre, id_documento_target=None):
    """Gatilla el motor para el documento seleccionado.
    Usa st.toast() para feedback y guarda estado en session_state
    para evitar errores DOM removeChild al escribir al sidebar durante renders.
    """
    if es_entorno_databricks():
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()

            job_id = None
            try:
                with open("databricks_config.yaml", "r", encoding="utf-8") as f:
                    db_config = yaml.safe_load(f)
                    job_id = db_config.get("databricks_job_id")
            except Exception:
                pass

            job_id = job_id or os.environ.get("DATABRICKS_JOB_ID")
            if not job_id:
                raise Exception("DATABRICKS_JOB_ID no configurada en variables de entorno o databricks_config.yaml")

            run = w.jobs.run_now(
                job_id=int(job_id),
                job_parameters={"nombre_documento": documento_nombre, "id_documento_target": id_documento_target or ""}
            )
            # Compatible con distintas versiones del SDK (objeto o dict)
            bound = run.bind()
            run_id = bound["run_id"] if isinstance(bound, dict) else bound.run_id
            st.session_state["ultimo_run_id"] = run_id
            st.session_state["ultimo_run_status"] = "success"
            st.toast(f"Job gatillado exitosamente. Run ID: {run_id}", icon="\U0001f680")
            return
        except Exception as e:
            st.session_state["ultimo_run_status"] = "error"
            st.session_state["ultimo_run_error"] = str(e)
            st.toast(f"Error al gatillar Job: {e}", icon="\u274c")
            return

    st.session_state["ultimo_run_status"] = "mock"
    st.toast(f"[MOCK] Ejecución enviada para '{documento_nombre}'.", icon="\u2705")
