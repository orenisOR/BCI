# Motor KQI Bci - Proyecto Apolo

Este repositorio contiene la implementación del Motor Inteligente de Calidad de Datos (KQI) desarrollado para Banco Bci, enfocado en auditar transcripciones de documentos del Proyecto Apolo.

## Estructura del Proyecto

- `app.py`: Aplicación Streamlit para la capa de configuración (Pilar 1). Permite definir reglas deterministas y ajustar pesos/modelos para la validación semántica con Gemini.
- `motor_kqi.py`: Script principal de orquestación y procesamiento con PySpark (Pilar 2). Integra SmartRouting para bifurcar la ejecución entre datos No Estructurados y Semiestructurados.
- `setup_database.sql`: Scripts DDL para la creación de esquemas, tablas y vistas en Databricks Unity Catalog (Pilar 3).
- `requirements.txt`: Dependencias de Python requeridas para ejecutar el proyecto.

## Ejecución Local (Mocks y Simulaciones)

El código ha sido diseñado para ser **agnóstico al entorno**. Localmente, simulará las conexiones a bases de datos y la lectura de datos mediante Mocks.

### 1. Instalación
```bash
pip install -r requirements.txt
```

### 2. Configuración (Pilar 1)
```bash
streamlit run app.py
```
*Esto abrirá la interfaz de Streamlit. Selecciona el flujo deseado y haz clic en Guardar. Creará un archivo `mock_config.json` localmente simulando a Unity Catalog.*

### 3. Procesamiento (Pilar 2)
```bash
python motor_kqi.py
```
*Este comando leerá `mock_config.json` y ejecutará la ruta correspondiente de PySpark. Para la ruta No Estructurada, intentará usar el modelo Gemini.*

**Nota sobre Gemini:**
Para usar el LLM de Gemini en la validación No Estructurada, el motor leerá automáticamente tu archivo `.env`.
- Asegúrate de que el archivo `.env` en la raíz tenga la línea: `GOOGLE_API_KEY="tu_api_key"`
- Si el archivo `.env` no existe o no tiene la llave, el sistema simulará una respuesta de evaluación (Mock LLM de forma segura).

## Invocación a Demanda del Motor KQI

Dado que la ejecución del Motor es a demanda, existen dos formas principales de gatillarlo (trigger) en producción:

### 1. Invocación por API REST (Integración de Sistemas)
Dado que el Bci tiene procesos automatizados, otro sistema del banco puede ser el que "empuje" la ejecución.
- **Cómo funciona:** Databricks expone un Endpoint (URL) único para nuestro Motor KQI asociado al Job.
- **El Gatillo:** Cuando el sistema origen del Proyecto Apolo termina de transcribir los documentos, hace una llamada POST a la API de Databricks (Ej. `Run Now API`). Esta llamada puede incluso incluir parámetros (ej. "Procesa solo los documentos de la fecha de hoy").

### 2. Invocación Manual desde el Front-end (Pilar 1)
Para dar control total a los usuarios de negocio (Data Stewards).
- **Cómo funciona:** En la aplicación `app.py` en Streamlit (Pilar 1), se ha agregado el botón **"Ejecutar Auditoría KQI"**.
- **El Gatillo:** Al presionar el botón, el código en Python utiliza el SDK de Databricks para enviar la orden de encendido al Motor (Pilar 2) utilizando las credenciales configuradas en `databricks_config.yaml`.

## Despliegue en Databricks

Al llevar este código a Databricks (mediante Repos/GitHub Sync):

1. **Pilar 1:** `app.py` puede desplegarse en **Databricks Apps** (disponible en ciertas regiones de Azure/AWS) o en una máquina virtual externa que conecte a Databricks SQL. Debes reemplazar la función de mock `guardar_configuracion()` por el conector real de `databricks.sql`.
2. **Pilar 2:** Sube `motor_kqi.py` como un **Job** de PySpark o ábrelo en un Notebook. Deberás reemplazar `os.environ.get("GOOGLE_API_KEY")` por la llamada segura `dbutils.secrets.get(scope="...", key="...")`.
3. **Pilar 3:** Ejecuta `setup_database.sql` en un Notebook o SQL Warehouse usando `%sql` para preparar el esquema.
