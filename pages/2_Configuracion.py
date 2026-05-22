import streamlit as st
import json
import datetime
from utils import generar_sidebar, cargar_configuracion_maestra, guardar_configuracion_maestra, es_administrador, es_entorno_databricks, ejecutar_auditoria_kqi

st.set_page_config(page_title="Configuración de Documento - KQI Bci", layout="wide", page_icon="⚙️")
generar_sidebar()

if "doc_actual" not in st.session_state or not st.session_state.doc_actual:
    st.error("No se ha seleccionado ningún documento para configurar. Por favor, vuelve al Inventario.")
    if st.button("⬅️ Ir al Inventario"):
        st.switch_page("app.py")
    st.stop()

doc_name = st.session_state.doc_actual
tipo_dato = st.session_state.naturaleza_actual

st.markdown(f'<h1 class="bci-gradient-title">⚙️ Configuración de Calidad: {doc_name}</h1>', unsafe_allow_html=True)
st.markdown(f"Gobernanza y Reglas de Negocio | Naturaleza: **{tipo_dato}**")
st.write("")

config_maestra = cargar_configuracion_maestra()
config_generada = {}

# Extraer opciones parametrizadas
try:
    with open("config_variables.json", "r", encoding="utf-8") as f:
        variables_config = json.load(f)
        operadores_lista = variables_config.get("operadores_matematicos", [">", "<", ">=", "<=", "==", "!="])
        modelos_disponibles = variables_config.get("modelos_llm", ["gemini-1.5-pro"])
        variables_no_estructurados = variables_config.get("kqi_no_estructurados", [])
except FileNotFoundError:
    operadores_lista = [">", "<", ">=", "<=", "==", "!="]
    modelos_disponibles = ["gemini-1.5-pro"]
    variables_no_estructurados = []

# ==========================================
# PROGRESSIVE DISCLOSURE Y PARAMETRIZACIÓN
# ==========================================
if "No Estructurado" in tipo_dato:
    st.subheader("Parametrización Semántica (FAISS + LLM)")
    with st.container():
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Pesos KQI (0.0 a 1.0)**")
            pesos_kqi = {}
            config_previa = config_maestra.get(doc_name, {}).get("parametros", {}).get("pesos_kqi", {})
            for var in variables_no_estructurados:
                val_previo = config_previa.get(var["id"], 0.5)
                pesos_kqi[var["id"]] = st.slider(var["nombre"], 0.0, 1.0, float(val_previo), 0.1)
                st.caption(f"Ajusta la sensibilidad del motor para medir {var['nombre'].lower()}.")
        
        with col2:
            st.markdown("**Configuración LLM (Gemini)**")
            mod_previo = config_maestra.get(doc_name, {}).get("parametros", {}).get("modelo_llm", modelos_disponibles[0])
            idx_modelo = modelos_disponibles.index(mod_previo) if mod_previo in modelos_disponibles else 0
            
            modelo_llm = st.selectbox("Selecciona el modelo a usar:", modelos_disponibles, index=idx_modelo)
            st.caption("Determina la versión del modelo fundacional para extraer el juicio de evaluación.")
            st.info("💡 En producción, el API Key será inyectada mediante `dbutils.secrets.get()`.")

    with st.container():
        st.markdown("---")
        st.markdown("### Configuración Avanzada")
        
        c3, c4 = st.columns(2)
        with c3:
            st.markdown("**Umbrales de Calidad (0.0 a 1.0)**")
            umbrales_previos = config_maestra.get(doc_name, {}).get("parametros", {}).get("umbrales", {
                "aprobacion_gold": 0.80, "revision_hitl": 0.70, "alerta_lote_critico": 0.85,
                "semaforo_verde": 0.80, "semaforo_amarillo": 0.50
            })
            
            aprobacion_gold = st.number_input("Umbral Capa Gold (Aprobación Automática)", 0.0, 1.0, float(umbrales_previos.get("aprobacion_gold", 0.80)), 0.05)
            revision_hitl = st.number_input("Umbral Revisión Humana (HITL)", 0.0, 1.0, float(umbrales_previos.get("revision_hitl", 0.70)), 0.05)
            alerta_lote = st.number_input("Alerta de Lote Crítico", 0.0, 1.0, float(umbrales_previos.get("alerta_lote_critico", 0.85)), 0.05)
            semaforo_v = st.number_input("Semáforo Verde PowerBI", 0.0, 1.0, float(umbrales_previos.get("semaforo_verde", 0.80)), 0.05)
            semaforo_a = st.number_input("Semáforo Amarillo PowerBI", 0.0, 1.0, float(umbrales_previos.get("semaforo_amarillo", 0.50)), 0.05)
            
            umbrales = {
                "aprobacion_gold": aprobacion_gold,
                "revision_hitl": revision_hitl,
                "alerta_lote_critico": alerta_lote,
                "semaforo_verde": semaforo_v,
                "semaforo_amarillo": semaforo_a
            }
            
        with c4:
            st.markdown("**Detección de Datos Personales (PII)**")
            st.caption("Configura reglas dinámicas de expresiones regulares (Regex) para identificar y auditar datos sensibles (PII).")
            
            pii_state_key = f"pii_{doc_name}"
            if pii_state_key not in st.session_state:
                # Cargar existentes de mock_config o establecer los por defecto
                st.session_state[pii_state_key] = config_maestra.get(doc_name, {}).get("parametros", {}).get("regex_pii", {
                    "rut_chileno": "\\b\\d{7,8}-[Kk0-9]\\b",
                    "correo": "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}",
                    "telefono": "\\+?569\\d{8}"
                })
            
            # Formulario para añadir nueva regla PII
            with st.expander("➕ Añadir Nueva Regla PII"):
                pii_nombre = st.text_input("Nombre del PII", placeholder="Ej. tarjeta_credito, direccion")
                pii_regex = st.text_input("Expresión Regular (Regex)", placeholder="Ej. \\b(?:\\d[ -]*?){13,16}\\b")
                if st.button("Añadir PII"):
                    if pii_nombre and pii_regex:
                        st.session_state[pii_state_key][pii_nombre] = pii_regex
                        st.success(f"PII '{pii_nombre}' añadido exitosamente.")
                        st.rerun()
                    else:
                        st.error("Por favor completa ambos campos.")
            
            # Mostrar PIIs actuales
            st.write("**PIIs Configurados:**")
            pii_dict_copy = dict(st.session_state[pii_state_key])
            if not pii_dict_copy:
                st.info("No hay PIIs configurados para este documento.")
            else:
                for pii_k, pii_val in pii_dict_copy.items():
                    col_p1, col_p2 = st.columns([5, 1])
                    col_p1.write(f"🔍 **{pii_k}**: `{pii_val}`")
                    if col_p2.button("🗑️", key=f"del_pii_{doc_name}_{pii_k}"):
                        del st.session_state[pii_state_key][pii_k]
                        st.success(f"PII '{pii_k}' eliminado.")
                        st.rerun()
            
            regex_pii = st.session_state[pii_state_key]

    config_generada = {
        "tipo_procesamiento": "No Estructurado",
        "parametros": {
            "pesos_kqi": pesos_kqi, 
            "modelo_llm": modelo_llm,
            "umbrales": umbrales,
            "regex_pii": regex_pii
        },
        "timestamp": str(datetime.datetime.now())
    }

else:
    st.subheader("Reglas de Validación Determinista")
    state_key = f"reglas_{doc_name}"
    
    if state_key not in st.session_state:
        st.session_state[state_key] = config_maestra.get(doc_name, {}).get("parametros", {}).get("reglas", [])
        
    with st.container():
        with st.form("form_regla"):
            st.markdown("**Añadir nueva regla de validación**")
            c1, c2, c3, c4 = st.columns(4)
            with c1: 
                atributo = st.text_input("Atributo a evaluar", placeholder="ej. Monto")
                st.caption("Ruta JSON")
            with c2: 
                operador = st.selectbox("Operador", operadores_lista)
                st.caption("Comparador")
            with c3: 
                valor = st.text_input("Valor esperado", placeholder="ej. 0")
                st.caption("Valor base")
            with c4: 
                obligatorio = st.checkbox("¿Es Obligatorio?", value=True)
                st.caption("Severidad")
                
            submitted = st.form_submit_button("Añadir Regla")
            if submitted and atributo and valor:
                nueva_regla = {"atributo": atributo, "operador": operador, "valor": valor, "obligatorio": obligatorio}
                if nueva_regla in st.session_state[state_key]:
                    st.error("⚠️ Esta regla ya existe en la lista.")
                else:
                    st.session_state[state_key].append(nueva_regla)
                    st.success("Regla añadida exitosamente.")
                    st.rerun()

        if st.session_state[state_key]:
            st.markdown("### Reglas Definidas")
            
            def eliminar_regla(indice):
                st.session_state[state_key].pop(indice)
                
            def limpiar_reglas():
                st.session_state[state_key] = []

            with st.container():
                for i, regla in enumerate(st.session_state[state_key]):
                    col_r1, col_r2 = st.columns([4, 1])
                    texto_regla = f"`{regla['atributo']} {regla['operador']} {regla['valor']}` | **Obligatorio:** {'Sí' if regla['obligatorio'] else 'No'}"
                    col_r1.write(texto_regla)
                    col_r2.button("🗑️ Eliminar", key=f"del_{doc_name}_{i}", on_click=eliminar_regla, args=(i,))
                
            st.write("") 
            st.button("Limpiar Todas las Reglas", on_click=limpiar_reglas)

    config_generada = {
        "tipo_procesamiento": "Semiestructurado",
        "parametros": {"reglas": st.session_state[state_key]},
        "timestamp": str(datetime.datetime.now())
    }

st.divider()

is_admin = es_administrador()
if not is_admin:
    st.error("🚫 No tienes permisos de administrador para guardar esta configuración.")

if st.button("Guardar Configuración de Documento", type="primary", disabled=not is_admin):
    config_maestra[doc_name] = config_generada
    guardar_configuracion_maestra(config_maestra)
    if not es_entorno_databricks():
        st.success(f"✅ Configuración de '{doc_name}' guardada localmente en mock_config.json.")

st.write("")
st.markdown("### Ejecución del Orquestador")
st.caption("Gatilla el procesamiento completo del pipeline desde aquí.")

if st.button("▶️ Ejecutar Motor KQI", type="secondary"):
    if es_entorno_databricks():
        with st.spinner("Gatillando Job en Databricks..."):
            ejecutar_auditoria_kqi(doc_name)
    else:
        with st.spinner("Ejecutando motor PySpark y generación de PowerBI local..."):
            import subprocess
            try:
                res = subprocess.run(["python", "motor_kqi.py", doc_name], capture_output=True, text=True)
                res2 = subprocess.run(["python", "generador_powerbi.py"], capture_output=True, text=True)
                if res.returncode == 0 and res2.returncode == 0:
                    st.success("✅ Motor ejecutado exitosamente. Los datos de PowerBI y la capa Gold/HITL local han sido actualizados.")
                else:
                    st.error("❌ Ocurrió un error al ejecutar los procesos locales.")
                with st.expander("Ver Logs de Ejecución"):
                    st.code(f"--- MOTOR LOGS ---\n{res.stdout}\n{res.stderr}\n\n--- POWERBI LOGS ---\n{res2.stdout}\n{res2.stderr}")
            except Exception as e:
                st.error(f"Error al ejecutar: {e}")
