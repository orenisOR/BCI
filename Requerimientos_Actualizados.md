# Especificación de Requerimientos y Directrices de Desarrollo Actualizada: Motor KQI Bci

## 1. Contexto del Proyecto y Reglas de Negocio
El objetivo es construir un Motor Inteligente de Calidad de Datos (KQI) para el Banco Bci, enfocado en auditar transcripciones de documentos (Proyecto Apolo) almacenadas en un Datalake.

- **Fuera de Alcance Estricto:** NO implementar OCR, NO implementar extracción de texto de PDFs, NO implementar enmascaramiento de PII (delegado a Azure), y NO implementar flujos de corrección humana (HITL). La solución solo detecta, mide y alerta.
- **Paradigma Dual (Smart Routing):** El sistema procesa datos de dos naturalezas distintas:
  - **No Estructurados:** Requieren Chunking, Base Vectorial FAISS y evaluación semántica mediante LLM para medir Coherencia, Ambigüedad y Entropía.
  - **Semiestructurados (JSON/Tablas):** Requieren aplanamiento de jerarquías y validación determinista (reglas matemáticas) sin usar el LLM, para optimización FinOps.

---

## 2. Restricciones Técnicas y Entorno de Despliegue

- **Agnosticismo Local (Implementado):** El proyecto corre localmente sin depender estrictamente de bases de datos externas gracias a la implementación de Mocks (`mock_config.json`, simulación de PySpark y DataFrames en memoria).
- **Frameworks Actuales:** 
  - `streamlit` para UI
  - `pyspark` para procesamiento masivo
  - `faiss-cpu` para base vectorial temporal
  - **MODIFICACIÓN APROBADA:** Integración con **Google Gemini** (`google-generativeai`) en lugar de Azure OpenAI para la evaluación del LLM-as-a-Judge.
- **Gestión de Secretos:** Preparado para utilizar variables de entorno localmente y transicionar a `dbutils.secrets.get()` una vez desplegado en Databricks.

---

## 3. Especificación Arquitectónica (Los 3 Pilares) y Estado Actual

### Pilar 1: Capa de Configuración (Mantenedor Front-end)
**Estado:** `COMPLETADO (Versión Local)`

**Desarrollo Actual:**
- **Arquitectura Multi-page:** Migración exitosa al flujo nativo de Streamlit con las páginas:
  - `app.py`: Actúa como Dashboard Principal / Inventario. Muestra la lista de documentos y posee un **nuevo gatillo manual** para ejecutar el motor de auditoría directamente.
  - `pages/1_Crear_Documento.py`: Formulario para registrar nuevos documentos, eligiendo su naturaleza (No Estructurado vs Semiestructurado).
  - `pages/2_Configuracion.py`: Interfaz detallada para configurar las reglas. Implementa *Progressive Disclosure* (muestra sliders de pesos KQI para No Estructurados o constructor de reglas para Semiestructurados).
- **Parametrización JSON:** Se eliminaron las listas *hardcodeadas*. Opciones como operadores matemáticos y modelos LLM se extraen de `config_variables.json`.
- **Persistencia:** Guardado local 100% funcional en `mock_config.json` para facilitar las pruebas agnósticas.

**Lo que resta por desarrollar / Modificaciones demandadas:**
- **Integración con Unity Catalog:** Cambiar la lógica de persistencia para que, al estar en producción, los datos se inserten/actualicen en la tabla real `kqi_configuracion`.
- **RBAC Activo:** Enlazar la validación `es_administrador()` al `current_user()` nativo de Databricks Apps, reemplazando el mock local actual en `utils.py`.

### Pilar 2: Capa de Orquestación y Procesamiento (Motor PySpark)
**Estado:** `PARCIALMENTE IMPLEMENTADO (Estructura Base y Mocks)`

**Desarrollo Actual:**
- **SmartRouter (`motor_kqi.py`):** Clase principal construida y operativa. Capaz de inicializar Spark (o actuar en modo fallback), bifurcando entre `procesar_no_estructurado` y `procesar_semiestructurado`.
- **Módulo de Ingesta:** Función `ingerir_y_aplanar()` implementada, utilizando `explode_outer()` para normalizar estructuras jerárquicas JSON.
- **Ruta No Estructurada:** Integración con `FAISS` definida (índice efímero). Se programó una User Defined Function (UDF) en PySpark para ejecutar el prompt al LLM Gemini por cada fila del DataFrame.
- **Integración Front-Motor:** La invocación manual desde el Frontend (app.py -> ejecutar_auditoria_kqi -> motor_kqi.ejecutar) funciona y enruta correctamente basándose en `nombre_documento`.

**Lo que resta por desarrollar / Modificaciones demandadas:**
- **Conexión Delta Lake:** Reemplazar los DataFrames generados en memoria (`spark.createDataFrame`) por lecturas de las tablas origen y escrituras en `kqi_resultados`.
- **Motor de Reglas Dinámico (Semiestructurado):** Completar el intérprete de reglas en PySpark. Actualmente procesa de forma simulada. Debe iterar sobre el JSON de configuración para construir dinámicamente las condiciones `.when(col(atributo) operador valor)` en tiempo de ejecución.
- **Invocación por API REST:** Empaquetar el script `motor_kqi.py` como un Job en Databricks y exponer su trigger mediante Databricks REST API para habilitar que el sistema origen (Apolo) invoque la auditoría automáticamente al finalizar la transcripción.

### Pilar 3: Capa de Visibilidad y Alertas
**Estado:** `MODELADO DDL COMPLETADO`

**Desarrollo Actual:**
- **Scripts SQL (`setup_database.sql`):** Creadas las sentencias DDL para establecer el esquema `bci_kqi_apolo`.
- Modelado de `kqi_configuracion` y `kqi_resultados` adaptado al "Drill-down Dual", incorporando la columna `nombre_tipo_documento` para trazabilidad de inventario.
- Vista materializada `vw_kqi_alertas_powerbi` lista para consumo BI.

**Lo que resta por desarrollar / Modificaciones demandadas:**
- **Ejecución en Databricks:** Correr físicamente el archivo `setup_database.sql` en un SQL Warehouse.
- **Construcción en Power BI:** Que el equipo analítico (o consultor BI) se conecte a esta vista para construir los paneles finales.

---

## 4. Plan de Acción Recomendado (Siguientes Pasos)
1. **Despliegue Databricks Apps:** Migrar los archivos del repositorio al entorno Databricks.
2. **Reemplazo de Mocks a Unity Catalog:** Ajustar las dependencias `databricks-sql-connector` para que la App interactúe con el DDL creado en Databricks.
3. **Desarrollo Lógica Dinámica PySpark:** Programar el evaluador recursivo en la capa Semiestructurada del Motor para evaluar reglas sobre DataFrames Delta.
4. **Pruebas End-to-End:** Validar el ciclo completo inyectando un documento real de prueba y corroborando la inserción de resultados en Power BI.
