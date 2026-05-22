-- Setup de Base de Datos para Motor KQI Apolo en Databricks Unity Catalog
-- Estos scripts deben ser ejecutados en un SQL Warehouse o Notebook de Databricks

-- Crear esquema/base de datos si no existe
CREATE SCHEMA IF NOT EXISTS bci_kqi_apolo;
USE bci_kqi_apolo;

-- Pilar 1: Capa de Configuración (Almacena los parámetros del Front-end)
CREATE TABLE IF NOT EXISTS kqi_configuracion (
    id_config STRING NOT NULL, -- UUID de la configuracion
    nombre_tipo_documento STRING NOT NULL, -- Nombre del Documento (ej. Contrato, Factura)
    tipo_procesamiento STRING NOT NULL, -- 'No Estructurado' o 'Semiestructurado'
    parametros_json STRING, -- JSON con reglas, umbrales y modelos (Gemini)
    usuario_modificacion STRING,
    fecha_modificacion TIMESTAMP
)
USING DELTA
COMMENT 'Tabla de configuración para el Enrutador Inteligente (SmartRouter)';

-- Pilar 3: Capa de Visibilidad y Alertas (Almacena los resultados del Motor PySpark)
CREATE TABLE IF NOT EXISTS kqi_resultados (
    id_ejecucion STRING NOT NULL,
    id_documento STRING NOT NULL,
    nombre_tipo_documento STRING NOT NULL, -- Identifica a qué tipo de documento pertenece
    tipo_procesamiento STRING NOT NULL, -- Identifica de qué ruta proviene
    score_global DOUBLE NOT NULL, -- Score unificado (1.0 o 0.0 para Semi, 0.0-1.0 para No Estructurado)
    detalle_error_llm STRING, -- Respuesta JSON de Gemini (Coherencia, Ambigüedad, Entropía)
    ruta_nodo_json_error STRING, -- JSONPath del nodo que falló la validación determinista
    fecha_procesamiento TIMESTAMP
)
USING DELTA
PARTITIONED BY (fecha_procesamiento)
COMMENT 'Tabla consolidada con los resultados duales para consumo en PowerBI';

-- Capa de Entrada: Transcripciones Silver a ser evaluadas
CREATE TABLE IF NOT EXISTS silver_transcripciones (
    id_documento STRING NOT NULL,
    nombre_documento STRING NOT NULL, -- Debe coincidir con nombre_tipo_documento
    Monto DOUBLE,                     -- Utilizado en procesamiento Semiestructurado (ej. Apolo 3)
    Estado STRING,                    -- Utilizado en procesamiento Semiestructurado (ej. Apolo 3)
    texto_transcrito STRING           -- Utilizado en procesamiento No Estructurado (ej. Apolo)
)
USING DELTA
COMMENT 'Tabla Silver de origen de transcripciones y campos clave';

-- Insertar Datos Mock en la Capa Silver para pruebas iniciales
INSERT INTO silver_transcripciones (id_documento, nombre_documento, Monto, Estado, texto_transcrito) VALUES
('doc_001_apolo3_bueno', 'Apolo 3', 1500.0, 'activo', NULL),
('doc_002_apolo3_malo', 'Apolo 3', -50.0, 'inactivo', NULL),
('doc_003_apolo_bueno', 'Apolo', NULL, NULL, 'CONTRATO DE ARRENDAMIENTO. En la ciudad de México, a 15 de mayo de 2026, comparecen por una parte el Arrendador y por otra el Arrendatario. Ambas partes acuerdan sujetarse a las cláusulas descritas en este documento con total claridad y coherencia legal.'),
('doc_004_apolo_malo', 'Apolo', NULL, NULL, 'C0N7RA70 D3 ARR3ND4M1ENT0... sfgsdfg. En 1a ciud4d de M3x1c0.. comparecen.. 00110001 error de lectura OCR... %$#');

-- Vista materializada/lógica recomendada para PowerBI
CREATE OR REPLACE VIEW vw_kqi_alertas_powerbi AS
SELECT 
    id_documento,
    nombre_tipo_documento,
    tipo_procesamiento,
    score_global,
    CASE 
        WHEN tipo_procesamiento = 'No Estructurado' THEN detalle_error_llm
        ELSE ruta_nodo_json_error
    END AS detalle_auditoria,
    fecha_procesamiento
FROM kqi_resultados
WHERE score_global < 0.8; -- Umbral de alerta por defecto

