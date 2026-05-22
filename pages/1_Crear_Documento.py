import streamlit as st
import json
import datetime
from utils import generar_sidebar, cargar_configuracion_maestra

st.set_page_config(page_title="Crear Nuevo Documento - KQI Bci", layout="wide", page_icon="➕")
generar_sidebar()

st.markdown('<h1 class="bci-gradient-title">➕ Crear Nuevo Tipo de Documento</h1>', unsafe_allow_html=True)
st.markdown("Registra un nuevo tipo de documento en el inventario del Proyecto Apolo para comenzar su gobernanza de calidad.")
st.write("")

config_maestra = cargar_configuracion_maestra()

try:
    with open("config_variables.json", "r", encoding="utf-8") as f:
        variables_config = json.load(f)
        nat_lista = variables_config.get("naturaleza_documentos", ["No Estructurados", "Semiestructurados"])
except FileNotFoundError:
    nat_lista = ["No Estructurados", "Semiestructurados"]

with st.container():
    with st.form("form_nuevo_doc"):
        nuevo_nombre = st.text_input("Nombre del Documento", placeholder="Ej. Contrato de Arriendo")
        st.caption("El nombre debe ser único e identificar claramente el documento a auditar.")
        
        nueva_naturaleza = st.selectbox("Naturaleza del Dato", nat_lista)
        st.caption("Determina si el documento se evaluará con FAISS+LLM o Validaciones Deterministas.")
        
        submitted_nuevo = st.form_submit_button("Crear Documento", type="primary")
        
        if submitted_nuevo and nuevo_nombre:
            if nuevo_nombre in config_maestra:
                st.error("Este tipo de documento ya existe.")
            else:
                tipo_proc = "No Estructurado" if "No Estructurado" in nueva_naturaleza else "Semiestructurado"
                config_maestra[nuevo_nombre] = {
                    "tipo_procesamiento": tipo_proc,
                    "parametros": {},
                    "timestamp": str(datetime.datetime.now())
                }
                with open("mock_config.json", "w", encoding="utf-8") as f:
                    json.dump(config_maestra, f, indent=4)
                
                # Redirigir al inventario
                st.success(f"Documento '{nuevo_nombre}' creado exitosamente.")
                st.switch_page("app.py")
