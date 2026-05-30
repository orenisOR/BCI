import streamlit as st
from utils import generar_sidebar, cargar_configuracion_maestra, ejecutar_auditoria_kqi, ejecutar_sql, es_entorno_databricks

st.set_page_config(page_title="Dashboard Inventario - KQI Bci", layout="wide", page_icon="📂")
generar_sidebar()

st.markdown('<h1 class="bci-gradient-title">📂 Inventario de Calidad de Datos (KQI)</h1>', unsafe_allow_html=True)
st.markdown("Administra los tipos de documentos del Proyecto Apolo y accede a la configuración avanzada de auditoría.")
st.write("")

config_maestra = cargar_configuracion_maestra()

if not config_maestra:
    st.info("No hay tipos de documentos configurados. Ve a 'Nuevo Documento' en el menú lateral para comenzar.")
else:
    st.subheader("Documentos en Producción")
    for doc_name, doc_data in config_maestra.items():
        tipo = doc_data.get('tipo_procesamiento', 'Desconocido')
        
        with st.container():
            st.markdown(f"""
            <div style="background: rgba(255, 255, 255, 0.02); border: 1px solid rgba(0, 229, 255, 0.12); border-radius: 12px; padding: 20px; margin-bottom: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.15);">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <h4 style="margin: 0; color: #00E5FF; font-weight: 700; font-family: 'Outfit';">📄 {doc_name}</h4>
                        <span style="font-size: 0.85em; color: #A0AEC0; font-style: italic; font-family: 'Outfit';">Naturaleza: {tipo}</span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            c1, c2 = st.columns([5, 1])
            with c2:
                if st.button("⚙️ Configurar", key=f"conf_{doc_name}", use_container_width=True):
                    st.session_state.doc_actual = doc_name
                    st.session_state.naturaleza_actual = tipo
                    st.switch_page("pages/2_Configuracion.py")
            st.write("")

    st.divider()
    
    # --- Sección de ejecución manual ---
    st.subheader("🚀 Ejecutar Motor de Auditoría")
    st.markdown("Gatilla manualmente la evaluación del motor KQI para auditar y promover documentos a las capas Gold/HITL.")
    
    lista_documentos = list(config_maestra.keys())
    
    st.markdown("""
    <div style="background: rgba(0, 229, 255, 0.03); border: 1px solid rgba(0, 229, 255, 0.2); border-radius: 12px; padding: 20px; margin-bottom: 16px;">
        <p style="margin: 0; font-weight: 600; color: #00E5FF;">💡 Invocación On-Demand (1-a-1)</p>
        <p style="margin: 0; font-size: 0.9em; color: #E2E8F0;">Selecciona el tipo de documento y opcionalmente un documento específico de la capa Silver para auditar.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Fila 1: Tipo de documento
    col_tipo, col_btn = st.columns([3, 1])
    with col_tipo:
        doc_a_ejecutar = st.selectbox("Tipo de documento:", lista_documentos, key="select_doc_ejecutar")
    with col_btn:
        st.write("")
    
    # Fila 2: Selector de id_documento_target (carga dinámica desde Silver)
    documentos_silver = []
    if es_entorno_databricks():
        try:
            rows = ejecutar_sql(
                f"SELECT DISTINCT id_documento FROM silver_transcripciones WHERE (estado_auditoria IS NULL OR estado_auditoria != 'procesado') AND nombre_documento = '{doc_a_ejecutar}' ORDER BY id_documento",
                fetch=True
            )
            documentos_silver = [r[0] for r in rows if r[0]]
        except Exception:
            pass
    
    col_doc, col_exec = st.columns([3, 1])
    with col_doc:
        opciones_doc = ["(Todos - procesar lote completo)"] + documentos_silver
        id_doc_seleccionado = st.selectbox(
            "Documento específico (Silver):",
            opciones_doc,
            key="select_id_doc_target",
            help="Selecciona un documento específico o deja en 'Todos' para procesar el lote completo."
        )
    with col_exec:
        st.write("")
        btn_ejecutar = st.button("▶️ Ejecutar Auditoría", type="primary", use_container_width=True, key="btn_ejecutar_motor")

    # Mostrar resultado de la última ejecución
    if st.session_state.get("ultimo_run_status") == "success":
        st.success(f"🚀 Último Job gatillado exitosamente. Run ID: {st.session_state.get('ultimo_run_id', 'N/A')}")
    elif st.session_state.get("ultimo_run_status") == "error":
        st.error(f"❌ Error en última ejecución: {st.session_state.get('ultimo_run_error', '')}")

    # Ejecutar DESPUÉS de renderizar todo el layout (evita DOM mismatch)
    if btn_ejecutar:
        id_target = None if id_doc_seleccionado.startswith("(Todos") else id_doc_seleccionado
        ejecutar_auditoria_kqi(doc_a_ejecutar, id_documento_target=id_target)
        st.rerun()
