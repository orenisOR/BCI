import streamlit as st
from utils import generar_sidebar, cargar_configuracion_maestra, ejecutar_auditoria_kqi

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
        
        # Tarjeta visual con estilo Bci
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
            
            # Botón de acción alineado
            c1, c2 = st.columns([5, 1])
            with c2:
                if st.button("⚙️ Configurar", key=f"conf_{doc_name}", use_container_width=True):
                    st.session_state.doc_actual = doc_name
                    st.session_state.naturaleza_actual = tipo
                    st.switch_page("pages/2_Configuracion.py")
            st.write("")

    st.divider()
    
    # Nueva sección de ejecución manual con diseño corporativo
    st.subheader("🚀 Ejecutar Motor de Auditoría")
    st.markdown("Gatilla manualmente la evaluación del motor KQI para auditar y promover documentos a las capas Gold/HITL.")
    
    lista_documentos = list(config_maestra.keys())
    
    with st.container():
        st.markdown("""
        <div style="background: rgba(0, 229, 255, 0.03); border: 1px solid rgba(0, 229, 255, 0.2); border-radius: 12px; padding: 20px; margin-bottom: 16px;">
            <p style="margin: 0; font-weight: 600; color: #00E5FF;">💡 Invocación Manual y Automática</p>
            <p style="margin: 0; font-size: 0.9em; color: #E2E8F0;">El motor procesará la capa Silver correspondiente, actualizando la capa Gold y enviando alertas automáticas a PowerBI.</p>
        </div>
        """, unsafe_allow_html=True)
        
        col_run1, col_run2 = st.columns([3, 1])
        with col_run1:
            doc_a_ejecutar = st.selectbox("Selecciona el documento a auditar:", lista_documentos)
        with col_run2:
            st.write("")  # Alineación
            if st.button("▶️ Ejecutar Auditoría", type="primary", use_container_width=True):
                ejecutar_auditoria_kqi(doc_a_ejecutar)
