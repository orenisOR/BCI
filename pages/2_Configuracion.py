import streamlit as st
import json
import datetime
from utils import generar_sidebar, cargar_configuracion_maestra, guardar_configuracion_maestra, es_administrador, es_entorno_databricks, ejecutar_auditoria_kqi, ejecutar_sql, ejecutar_sql

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
# Arquitectura Plug-and-Play: pesos_kqi jerárquico
# ==========================================
if "No Estructurado" in tipo_dato:
    st.subheader("🔌 Configuración de Variables KQI (Plugin Architecture)")
    st.caption("Cada variable puede evaluarse por LLM, por un evaluador determinístico registrado, o ser una categoría compuesta de sub-elementos.")

    # Cargar opciones del config
    evaluadores_det = variables_config.get("evaluadores_deterministas", [])
    subelementos_catalogo = variables_config.get("subelementos_disponibles", [])
    fuentes_disponibles = variables_config.get("fuentes_disponibles", ["llm", "determinista", "compuesta"])
    polaridades = variables_config.get("polaridades", ["positiva", "negativa"])
    metodos_ids = [e["id"] for e in evaluadores_det]

    # Cargar config previa (soporta formato jerárquico y legacy)
    config_previa = config_maestra.get(doc_name, {}).get("parametros", {}).get("pesos_kqi", {})

    # State key para variables dinámicas
    vars_state_key = f"vars_kqi_{doc_name}"
    if vars_state_key not in st.session_state:
        # Inicializar desde config previa
        st.session_state[vars_state_key] = config_previa if config_previa else {}

    pesos_kqi = {}

    # --- Sección: Variables KQI principales ---
    st.markdown("### Variables de Evaluación")

    # Selector de variables activas para este documento
    # Variables compuestas siempre se incluyen; las semánticas/deterministas son seleccionables
    vars_compuestas = [v for v in variables_no_estructurados if v.get("fuente_default") == "compuesta"]
    vars_seleccionables = [v for v in variables_no_estructurados if v.get("fuente_default") != "compuesta"]

    # Determinar cuáles estaban activas previamente
    ids_previos_activos = list(config_previa.keys()) if config_previa else [v["id"] for v in vars_seleccionables]
    ids_seleccionables = [v["id"] for v in vars_seleccionables]
    default_seleccion = [vid for vid in ids_previos_activos if vid in ids_seleccionables]

    vars_activas_ids = st.multiselect(
        "Variables semánticas activas para este documento:",
        options=ids_seleccionables,
        default=default_seleccion,
        format_func=lambda vid: next((v["nombre"] for v in vars_seleccionables if v["id"] == vid), vid),
        key=f"multiselect_vars_{doc_name}",
        help="Selecciona qué variables de calidad aplican a este tipo de documento. Las compuestas siempre se incluyen."
    )

    # Combinar: compuestas (siempre) + seleccionadas
    variables_activas = vars_compuestas + [v for v in vars_seleccionables if v["id"] in vars_activas_ids]

    for var in variables_activas:
        var_id = var["id"]
        var_nombre = var["nombre"]
        var_desc = var.get("descripcion", "")
        fuente_default = var.get("fuente_default", "llm")

        # Leer config previa para esta variable
        prev = config_previa.get(var_id, {})
        if isinstance(prev, (int, float)):
            # Legacy format: convertir
            prev = {"peso": float(prev), "fuente": "llm"}

        with st.expander(f"📊 **{var_nombre}** — {var_desc}", expanded=True):
            col_peso, col_fuente = st.columns([1, 2])

            with col_peso:
                peso_val = st.slider(
                    f"Peso ({var_id})", 0.0, 1.0,
                    float(prev.get("peso", 0.3)), 0.05,
                    key=f"peso_{var_id}"
                )

            with col_fuente:
                # Variables compuestas no muestran selector — siempre son "compuesta"
                if fuente_default == "compuesta":
                    fuente_val = "compuesta"
                    st.markdown(f"**Fuente:** `compuesta` *(derivada de subelementos)*")
                else:
                    fuente_prev = prev.get("fuente", fuente_default)
                    idx_fuente = fuentes_disponibles.index(fuente_prev) if fuente_prev in fuentes_disponibles else 0
                    fuente_val = st.selectbox(
                        f"Fuente de evaluación ({var_id})",
                        fuentes_disponibles,
                        index=idx_fuente,
                        key=f"fuente_{var_id}"
                    )

            # --- Fuente: LLM ---
            if fuente_val == "llm":
                pesos_kqi[var_id] = {"peso": peso_val, "fuente": "llm"}

            # --- Fuente: Determinista ---
            elif fuente_val == "determinista":
                col_met, col_pol = st.columns(2)
                with col_met:
                    metodo_prev = prev.get("metodo", metodos_ids[0] if metodos_ids else "regex_ratio")
                    idx_met = metodos_ids.index(metodo_prev) if metodo_prev in metodos_ids else 0
                    metodo_val = st.selectbox(
                        f"Método evaluador ({var_id})", metodos_ids, index=idx_met,
                        key=f"metodo_{var_id}"
                    )
                    # Mostrar descripción del evaluador
                    eval_info = next((e for e in evaluadores_det if e["id"] == metodo_val), None)
                    if eval_info:
                        st.caption(eval_info.get("descripcion", ""))

                with col_pol:
                    pol_prev = prev.get("polaridad", prev.get("params", {}).get("polaridad", "negativa"))
                    idx_pol = polaridades.index(pol_prev) if pol_prev in polaridades else 1
                    polaridad_val = st.selectbox(
                        f"Polaridad ({var_id})", polaridades, index=idx_pol,
                        key=f"pol_{var_id}"
                    )

                # Parámetros específicos del evaluador
                params_kqi = {"polaridad": polaridad_val}
                if eval_info:
                    param_keys = [p for p in eval_info.get("params", []) if p != "polaridad"]
                    if param_keys:
                        st.markdown("**Parámetros de calibración:**")
                        prev_params = prev.get("params", {})
                        cols_params = st.columns(len(param_keys))
                        for i, pk in enumerate(param_keys):
                            with cols_params[i]:
                                params_kqi[pk] = st.text_input(
                                    pk, value=str(prev_params.get(pk, "")),
                                    key=f"param_{var_id}_{pk}"
                                )

                pesos_kqi[var_id] = {
                    "peso": peso_val, "fuente": "determinista",
                    "metodo": metodo_val, "polaridad": polaridad_val,
                    "params": params_kqi
                }

            # --- Fuente: Compuesta ---
            elif fuente_val == "compuesta":
                st.markdown("**Sub-elementos:**")
                subelementos = {}
                prev_subs = prev.get("subelementos", {})

                # Permitir agregar sub-elementos del catálogo
                sub_state_key = f"subs_{doc_name}_{var_id}"
                if sub_state_key not in st.session_state:
                    if prev_subs:
                        st.session_state[sub_state_key] = list(prev_subs.keys())
                    else:
                        st.session_state[sub_state_key] = [s["id"] for s in subelementos_catalogo[:2]]

                # Selector de sub-elementos activos
                sub_opciones = [s["id"] for s in subelementos_catalogo]
                subs_activos = st.multiselect(
                    f"Sub-elementos activos para {var_nombre}",
                    sub_opciones,
                    default=st.session_state[sub_state_key],
                    key=f"multi_sub_{var_id}"
                )
                st.session_state[sub_state_key] = subs_activos

                for sub_id in subs_activos:
                    sub_cat = next((s for s in subelementos_catalogo if s["id"] == sub_id), None)
                    if not sub_cat:
                        continue
                    sub_prev = prev_subs.get(sub_id, {})

                    with st.container():
                        st.markdown(f"---")
                        st.markdown(f"**└ {sub_cat['nombre']}** — _{sub_cat.get('descripcion', '')}_")
                        cs1, cs2, cs3 = st.columns(3)

                        with cs1:
                            sub_peso = st.slider(
                                f"Peso relativo ({sub_id})", 0.0, 1.0,
                                float(sub_prev.get("peso", 0.5)), 0.1,
                                key=f"subpeso_{var_id}_{sub_id}"
                            )
                        with cs2:
                            sub_metodo_def = sub_cat.get("metodo_default", "regex_ratio")
                            sub_met_prev = sub_prev.get("metodo", sub_metodo_def)
                            idx_sm = metodos_ids.index(sub_met_prev) if sub_met_prev in metodos_ids else 0
                            sub_metodo = st.selectbox(
                                f"Método ({sub_id})", metodos_ids, index=idx_sm,
                                key=f"submet_{var_id}_{sub_id}"
                            )
                        with cs3:
                            sub_pol_prev = sub_prev.get("polaridad", "negativa")
                            idx_sp = polaridades.index(sub_pol_prev) if sub_pol_prev in polaridades else 1
                            sub_pol = st.selectbox(
                                f"Polaridad ({sub_id})", polaridades, index=idx_sp,
                                key=f"subpol_{var_id}_{sub_id}"
                            )

                        # Parámetros del sub-elemento
                        sub_eval_info = next((e for e in evaluadores_det if e["id"] == sub_metodo), None)
                        sub_params = {"polaridad": sub_pol}
                        if sub_eval_info:
                            sub_param_keys = [p for p in sub_eval_info.get("params", []) if p != "polaridad"]
                            if sub_param_keys:
                                prev_sub_params = sub_prev.get("params", {})
                                cols_sp = st.columns(len(sub_param_keys))
                                for i, spk in enumerate(sub_param_keys):
                                    with cols_sp[i]:
                                        sub_params[spk] = st.text_input(
                                            f"{spk} ({sub_id})",
                                            value=str(prev_sub_params.get(spk, "")),
                                            key=f"subparam_{var_id}_{sub_id}_{spk}"
                                        )

                        subelementos[sub_id] = {
                            "peso": sub_peso,
                            "fuente": "determinista",
                            "metodo": sub_metodo,
                            "polaridad": sub_pol,
                            "params": sub_params
                        }

                pesos_kqi[var_id] = {
                    "peso": peso_val, "fuente": "compuesta",
                    "subelementos": subelementos
                }

    # --- Sección: Modelo LLM ---
    st.markdown("---")
    st.markdown("### Configuración LLM")
    col_llm1, col_llm2 = st.columns(2)
    with col_llm1:
        mod_previo = config_maestra.get(doc_name, {}).get("parametros", {}).get("modelo_llm", modelos_disponibles[0])
        idx_modelo = modelos_disponibles.index(mod_previo) if mod_previo in modelos_disponibles else 0
        modelo_llm = st.selectbox("Modelo LLM para evaluación semántica:", modelos_disponibles, index=idx_modelo)
    with col_llm2:
        st.info("💡 El motor usa el plugin `llm_gemini` registrado en la Factory. API Key inyectada vía `dbutils.secrets`.")

    # --- Sección: Umbrales ---
    st.markdown("---")
    st.markdown("### Umbrales de Calidad")
    c3, c4 = st.columns(2)
    with c3:
        umbrales_previos = config_maestra.get(doc_name, {}).get("parametros", {}).get("umbrales", {
            "aprobacion_gold": 0.80, "revision_hitl": 0.70, "alerta_lote_critico": 0.85,
            "semaforo_verde": 0.80, "semaforo_amarillo": 0.50
        })
        aprobacion_gold = st.number_input("Umbral Capa Gold (Aprobación Automática)", 0.0, 1.0, float(umbrales_previos.get("aprobacion_gold", 0.80)), 0.05)
        revision_hitl = st.number_input("Umbral Revisión Humana (HITL)", 0.0, 1.0, float(umbrales_previos.get("revision_hitl", 0.70)), 0.05)
        alerta_lote = st.number_input("Alerta de Lote Crítico", 0.0, 1.0, float(umbrales_previos.get("alerta_lote_critico", 0.85)), 0.05)

    with c4:
        semaforo_v = st.number_input("Semáforo Verde PowerBI", 0.0, 1.0, float(umbrales_previos.get("semaforo_verde", 0.80)), 0.05)
        semaforo_a = st.number_input("Semáforo Amarillo PowerBI", 0.0, 1.0, float(umbrales_previos.get("semaforo_amarillo", 0.50)), 0.05)

    umbrales = {
        "aprobacion_gold": aprobacion_gold,
        "revision_hitl": revision_hitl,
        "alerta_lote_critico": alerta_lote,
        "semaforo_verde": semaforo_v,
        "semaforo_amarillo": semaforo_a
    }

    # --- Sección: PII ---
    st.markdown("---")
    st.markdown("### Detección de Datos Personales (PII)")
    st.caption("Reglas dinámicas de Regex para identificar datos sensibles.")

    pii_state_key = f"pii_{doc_name}"
    if pii_state_key not in st.session_state:
        st.session_state[pii_state_key] = config_maestra.get(doc_name, {}).get("parametros", {}).get("regex_pii", {
            "rut_chileno": "\\b\\d{7,8}-[Kk0-9]\\b",
            "correo": "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}",
            "telefono": "\\+?569\\d{8}"
        })

    with st.expander("➕ Añadir Nueva Regla PII"):
        pii_nombre = st.text_input("Nombre del PII", placeholder="Ej. tarjeta_credito")
        pii_regex = st.text_input("Expresión Regular", placeholder="Ej. \\b(?:\\d[ -]*?){13,16}\\b")
        if st.button("Añadir PII"):
            if pii_nombre and pii_regex:
                st.session_state[pii_state_key][pii_nombre] = pii_regex
                st.success(f"PII '{pii_nombre}' añadido.")
                st.rerun()
            else:
                st.error("Completa ambos campos.")

    pii_dict_copy = dict(st.session_state[pii_state_key])
    if pii_dict_copy:
        for pii_k, pii_val in pii_dict_copy.items():
            col_p1, col_p2 = st.columns([5, 1])
            col_p1.write(f"🔍 **{pii_k}**: `{pii_val}`")
            if col_p2.button("🗑️", key=f"del_pii_{doc_name}_{pii_k}"):
                del st.session_state[pii_state_key][pii_k]
                st.rerun()

    regex_pii = st.session_state[pii_state_key]

    # --- Preview JSON generado ---
    st.markdown("---")
    with st.expander("🔍 Preview: JSON de configuración que se guardará"):
        st.json(pesos_kqi)

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
st.caption("Gatilla la auditoría On-Demand para el documento seleccionado.")

# Selector de documento específico desde Silver
documentos_silver = []
if es_entorno_databricks():
    try:
        rows = ejecutar_sql(
            f"SELECT DISTINCT id_documento FROM silver_transcripciones WHERE (estado_auditoria IS NULL OR estado_auditoria != 'procesado') AND nombre_documento = '{doc_name}' ORDER BY id_documento",
            fetch=True
        )
        documentos_silver = [r[0] for r in rows if r[0]]
    except Exception:
        pass

opciones_doc = ["(Todos - lote completo)"] + documentos_silver
id_doc_seleccionado = st.selectbox(
    "Documento a auditar (Silver):",
    opciones_doc,
    key="select_id_doc_config",
    help="Selecciona un documento específico o 'Todos' para procesar el lote completo."
)

# Mostrar estado de última ejecución (estable, sin mutar DOM)
if st.session_state.get("ultimo_run_status") == "success":
    st.success(f"🚀 Último Job gatillado. Run ID: {st.session_state.get('ultimo_run_id', 'N/A')}")
elif st.session_state.get("ultimo_run_status") == "error":
    st.error(f"❌ {st.session_state.get('ultimo_run_error', 'Error desconocido')}")

btn_motor = st.button("▶️ Ejecutar auditoría", type="secondary", key="btn_motor_config")

# Ejecutar DESPUÉS de todo el layout para evitar removeChild
if btn_motor:
    id_target = None if id_doc_seleccionado.startswith("(Todos") else id_doc_seleccionado
    ejecutar_auditoria_kqi(doc_name, id_documento_target=id_target)
    st.rerun()
