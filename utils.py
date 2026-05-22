import streamlit as st
import json
import os
import yaml

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
        "DATABRICKS_RUNTIME_VERSION" in os.environ or 
        "DATABRICKS_APP_PORT" in os.environ or
        "DATABRICKS_APP_NAME" in os.environ
    )

def obtener_conexion_databricks():
    """Establece conexión a Databricks SQL Warehouse usando databricks-sql-connector."""
    from databricks import sql
    import yaml
    
    server_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not (server_hostname and http_path and token):
        try:
            with open("databricks_config.yaml", "r", encoding="utf-8") as f:
                db_config = yaml.safe_load(f)
                server_hostname = server_hostname or db_config.get("databricks_server_hostname")
                http_path = http_path or db_config.get("databricks_http_path")
                token = token or db_config.get("databricks_token")
        except Exception:
            pass
            
    if not (server_hostname and http_path and token):
        raise Exception("Credenciales de Databricks no configuradas.")
        
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=token
    )

def obtener_usuario_actual():
    """Recupera el current_user() desde el entorno de Databricks."""
    try:
        from streamlit import context
        headers = context.headers
        if "X-Forwarded-Email" in headers: return headers["X-Forwarded-Email"]
        if "DATABRICKS_CURRENT_USER" in os.environ: return os.environ["DATABRICKS_CURRENT_USER"]
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
    
    st.sidebar.markdown(f"👤 **Usuario:**\n`{usuario}`\n\n🛡️ **Perfil:** `{perfil}`")
    st.sidebar.divider()
    st.sidebar.markdown("**Navegación**")
    st.sidebar.page_link("app.py", label="📂 Dashboard Inventario")
    st.sidebar.page_link("pages/1_Crear_Documento.py", label="➕ Nuevo Documento")

def cargar_configuracion_maestra():
    if es_entorno_databricks():
        try:
            conn = obtener_conexion_databricks()
            cursor = conn.cursor()
            # Aseguramos existencia de la tabla antes de consultar
            cursor.execute("CREATE SCHEMA IF NOT EXISTS bci_kqi_apolo")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bci_kqi_apolo.kqi_configuracion (
                    id_config STRING NOT NULL,
                    nombre_tipo_documento STRING NOT NULL,
                    tipo_procesamiento STRING NOT NULL,
                    parametros_json STRING,
                    usuario_modificacion STRING,
                    fecha_modificacion TIMESTAMP
                ) USING DELTA
            """)
            
            cursor.execute("SELECT nombre_tipo_documento, tipo_procesamiento, parametros_json, fecha_modificacion FROM bci_kqi_apolo.kqi_configuracion")
            rows = cursor.fetchall()
            maestra = {}
            for row in rows:
                nombre = row[0]
                tipo_proc = row[1]
                params = json.loads(row[2]) if row[2] else {}
                fecha = str(row[3]) if row[3] else ""
                maestra[nombre] = {
                    "tipo_procesamiento": tipo_proc,
                    "parametros": params,
                    "timestamp": fecha
                }
            cursor.close()
            conn.close()
            return maestra
        except Exception as e:
            st.sidebar.warning(f"⚠️ Databricks SQL error (usando fallback mock local): {e}")

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
            conn = obtener_conexion_databricks()
            cursor = conn.cursor()
            usuario = obtener_usuario_actual()
            import datetime
            import uuid
            
            cursor.execute("CREATE SCHEMA IF NOT EXISTS bci_kqi_apolo")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bci_kqi_apolo.kqi_configuracion (
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
                
                # MERGE compatible con Databricks
                cursor.execute(f"""
                    MERGE INTO bci_kqi_apolo.kqi_configuracion AS target
                    USING (
                        SELECT 
                            '{nombre}' AS nombre_tipo_documento,
                            '{tipo_proc}' AS tipo_procesamiento,
                            '{params.replace("'", "''")}' AS parametros_json,
                            '{usuario}' AS usuario_modificacion,
                            CAST('{timestamp_str}' AS TIMESTAMP) AS fecha_modificacion
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
                        VALUES ('{id_config}', source.nombre_tipo_documento, source.tipo_procesamiento, source.parametros_json, source.usuario_modificacion, source.fecha_modificacion)
                """)
            conn.commit()
            cursor.close()
            conn.close()
            st.success("✅ Configuración guardada en Unity Catalog (bci_kqi_apolo.kqi_configuracion)")
            return
        except Exception as e:
            st.error(f"❌ Error en Databricks: {e}. Guardando copia en local...")
            
    with open("mock_config.json", "w", encoding="utf-8") as f:
        json.dump(maestra, f, indent=4, ensure_ascii=False)

def ejecutar_auditoria_kqi(documento_nombre):
    """Gatilla el motor para el documento seleccionado."""
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
                job_parameters={"nombre_documento": documento_nombre}
            )
            st.sidebar.success(f"🚀 Job gatillado en Databricks: Run ID {run.bind().run_id}")
            return
        except Exception as e:
            st.sidebar.error(f"❌ Error al gatillar Job en Databricks: {e}")
            return

    try:
        raise Exception("Entorno local detectado. Ejecución del SDK omitida.")
    except Exception as e:
        st.sidebar.warning(f"⚠️ {e}")
        st.sidebar.success(f"✅ [MOCK] Ejecución enviada para '{documento_nombre}'.")

