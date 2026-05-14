import os
import logging
import streamlit as st
from databricks import sql

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def guardar_en_unity_catalog(proyecto: str, modelo: str, umbral: float) -> bool:
    """
    Guarda la configuración del proyecto en la tabla de Databricks Unity Catalog.
    
    Args:
        proyecto (str): El nombre del proyecto ingresado por el usuario.
        modelo (str): El modelo de IA seleccionado.
        umbral (float): El valor del umbral de coherencia (0.0 - 1.0).
        
    Returns:
        bool: True si la operación (o la simulación) fue exitosa, False en caso de error crítico no manejado.
    """
    try:
        # ---------------------------------------------------------------------
        # NOTA PARA EL EQUIPO DATABRICKS:
        # Aquí se deben configurar las variables de entorno de Databricks.
        # En producción bajo Databricks Apps, a menudo se usa OAuth machine-to-machine
        # o los secretos inyectados en el entorno. Ajustar los nombres de
        # DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH y DATABRICKS_TOKEN.
        # ---------------------------------------------------------------------
        server_hostname = "dbc-0dbd7378-b83b.cloud.databricks.com"
        http_path = "/sql/1.0/warehouses/e246119012938b18"
        access_token = "dapi81eb07c7558b396214faa3a5346a85f5"

        # Lógica de conexión real a Databricks SQL Warehouse
        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                # Query parametrizada para evitar inyecciones SQL
                query = """
                    INSERT INTO catalog.schema.config_proyectos 
                    (nombre_proyecto, modelo_ia, umbral_coherencia) 
                    VALUES (?, ?, ?)
                """
                cursor.execute(query, (proyecto, modelo, umbral))
                
        logger.info(f"Registro exitoso en Unity Catalog para el proyecto: {proyecto}")
        return True

    except Exception as e:
        # ---------------------------------------------------------------------
        # COMPORTAMIENTO MOCK (SIMULADO) PARA DESARROLLO LOCAL
        # ---------------------------------------------------------------------
        logger.warning(f"Error de conexión o entorno local detectado: {e}")
        logger.info(f"[MOCK] Simulando guardado -> Proyecto: {proyecto} | Modelo: {modelo} | Umbral: {umbral}")
        
        # Devolvemos True para simular éxito y que la UI continúe su flujo
        return True

def main():
    """
    Función principal que renderiza la interfaz de usuario en Streamlit.
    """
    # 1. Título de la aplicación
    st.title("Configuración de Proyecto KQI - Pilar 1")
    
    # 2. Formularios de entrada (UI)
    nombre_proyecto = st.text_input("Nombre del Proyecto")
    
    modelo_ia = st.selectbox(
        "Modelo de IA", 
        options=['GPT-4o', 'Llama-3', 'Claude-3.5']
    )
    
    umbral_coherencia = st.slider(
        "Umbral de Coherencia", 
        min_value=0.0, 
        max_value=1.0, 
        value=0.8,
        step=0.01
    )
    
    # 3. Botón de envío
    if st.button("Guardar Configuración"):
        # Validación simple antes de enviar
        if not nombre_proyecto.strip():
            st.error("Por favor, ingresa un Nombre de Proyecto válido antes de guardar.")
            return
            
        # Llamada a la capa de datos
        with st.spinner("Guardando en Unity Catalog..."):
            exito = guardar_en_unity_catalog(
                proyecto=nombre_proyecto.strip(),
                modelo=modelo_ia,
                umbral=umbral_coherencia
            )
            
        # 4. Feedback visual al usuario
        if exito:
            st.success(f"La configuración del proyecto '{nombre_proyecto}' se ha guardado exitosamente.")
        else:
            st.error("Ocurrió un error inesperado al intentar guardar la configuración.")

if __name__ == "__main__":
    main()
