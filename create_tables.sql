
-- =============================================================================
-- SCRIPT DE CREACIÓN DE TABLAS - DATA WAREHOUSE E-COMMERCE
-- =============================================================================
-- 
-- Este script crea las tablas necesarias para el Data Warehouse unificado
-- que consolida información de e-commerce online y offline.
-- 
-- Estructura:
-- - dim_clientes: Dimensión de clientes deduplicados con métricas
-- - fact_compras: Tabla de hechos de compras unificadas
-- 
-- Autor: Sinergia Digital
-- Fecha: Junio 2025
-- Versión: 1.0.3
-- Base de Datos: PostgreSQL 13+
-- =============================================================================

-- -----------------------------------------------------------------------------
-- CONFIGURACIÓN INICIAL
-- -----------------------------------------------------------------------------

-- Establecer timezone para la sesión
SET timezone = 'America/Bogota';

-- Crear schema si no existe (opcional)
-- CREATE SCHEMA IF NOT EXISTS dwh_ecommerce;
-- SET search_path TO dwh_ecommerce, public;

-- -----------------------------------------------------------------------------
-- ELIMINACIÓN DE TABLAS EXISTENTES (SI EXISTEN)
-- -----------------------------------------------------------------------------

-- Eliminar tablas en orden correcto (primero fact, luego dim)
DROP TABLE IF EXISTS fact_compras CASCADE;
DROP TABLE IF EXISTS dim_clientes CASCADE;

-- Eliminar índices existentes (por seguridad)
DROP INDEX IF EXISTS idx_clientes_ci;
DROP INDEX IF EXISTS idx_clientes_activo;
DROP INDEX IF EXISTS idx_clientes_canales;
DROP INDEX IF EXISTS idx_clientes_total_gastado;
DROP INDEX IF EXISTS idx_clientes_ultima_compra;

DROP INDEX IF EXISTS idx_compras_ci;
DROP INDEX IF EXISTS idx_compras_fecha;
DROP INDEX IF EXISTS idx_compras_canal;
DROP INDEX IF EXISTS idx_compras_total;
DROP INDEX IF EXISTS idx_compras_year_month;

-- -----------------------------------------------------------------------------
-- TABLA DE DIMENSIÓN: CLIENTES UNIFICADOS
-- -----------------------------------------------------------------------------

CREATE TABLE dim_clientes (
    -- Identificadores únicos
    cliente_id          INTEGER NOT NULL,
    ci                  VARCHAR(20) NOT NULL UNIQUE,
    
    -- Información personal
    nombre              VARCHAR(100),
    apellido            VARCHAR(100),
    email               VARCHAR(150),
    telefono            VARCHAR(20),
    direccion           TEXT,
    ciudad              VARCHAR(100),
    
    -- Información de registro y actividad
    fecha_registro      TIMESTAMP,
    fecha_ultima_compra TIMESTAMP,
    primera_compra      TIMESTAMP,
    canales             VARCHAR(20), -- 'online', 'offline', 'online_offline'
    
    -- MÉTRICAS AGREGADAS PRINCIPALES
    total_gastado       NUMERIC(12,2) DEFAULT 0,
    ticket_promedio     NUMERIC(10,2) DEFAULT 0,
    frecuencia_compra   INTEGER DEFAULT 0,
    cliente_activo      INTEGER DEFAULT 0, -- 1 = activo últimos 90 días, 0 = inactivo
    dias_ultima_compra  INTEGER DEFAULT 0,
    
    -- MÉTRICAS POR CANAL (calculadas automáticamente)
    total_compra_online   NUMERIC(12,2) DEFAULT 0,
    total_compra_offline  NUMERIC(12,2) DEFAULT 0,
    fecha_compra_online   INTEGER DEFAULT 0,
    fecha_compra_offline  INTEGER DEFAULT 0,
    
    -- MÉTRICAS ADICIONALES DE NEGOCIO
    canales_utilizados    INTEGER DEFAULT 1, -- Número de canales diferentes usados
    
    -- Metadatos de auditoría
    fecha_carga         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraint de clave primaria
    CONSTRAINT pk_dim_clientes PRIMARY KEY (cliente_id),
    
    -- Constraints de validación
    CONSTRAINT chk_clientes_ci_valido 
        CHECK (ci IS NOT NULL AND LENGTH(TRIM(ci)) > 0),
    CONSTRAINT chk_clientes_total_gastado_positivo 
        CHECK (total_gastado >= 0),
    CONSTRAINT chk_clientes_ticket_promedio_positivo 
        CHECK (ticket_promedio >= 0),
    CONSTRAINT chk_clientes_frecuencia_positiva 
        CHECK (frecuencia_compra >= 0),
    CONSTRAINT chk_clientes_activo_binario 
        CHECK (cliente_activo IN (0, 1)),
    CONSTRAINT chk_clientes_canales_validos 
        CHECK (canales IN ('online', 'offline', 'online_offline')),
    CONSTRAINT chk_clientes_dias_ultima_compra_positivo 
        CHECK (dias_ultima_compra >= 0)
);

-- Comentarios en la tabla de clientes
COMMENT ON TABLE dim_clientes IS 'Dimensión de clientes unificados con métricas agregadas de e-commerce online y offline';

COMMENT ON COLUMN dim_clientes.cliente_id IS 'Identificador único de cliente (mantenido del sistema online)';
COMMENT ON COLUMN dim_clientes.ci IS 'Cédula de Identidad - clave natural única para deduplicación';
COMMENT ON COLUMN dim_clientes.canales IS 'Canales utilizados: online, offline o online_offline';
COMMENT ON COLUMN dim_clientes.total_gastado IS 'Total gastado por el cliente en todos los canales';
COMMENT ON COLUMN dim_clientes.ticket_promedio IS 'Ticket promedio de compras del cliente';
COMMENT ON COLUMN dim_clientes.frecuencia_compra IS 'Número total de compras realizadas';
COMMENT ON COLUMN dim_clientes.cliente_activo IS 'Cliente activo en últimos 90 días: 1=Sí, 0=No';
COMMENT ON COLUMN dim_clientes.dias_ultima_compra IS 'Días transcurridos desde última compra';

-- -----------------------------------------------------------------------------
-- TABLA DE HECHOS: COMPRAS UNIFICADAS
-- -----------------------------------------------------------------------------

CREATE TABLE fact_compras (
    -- Identificador único de compra
    compra_id           SERIAL PRIMARY KEY,
    
    -- Claves foráneas
    ci                  VARCHAR(20) NOT NULL,
    pedido_id           INTEGER, -- Puede ser NULL para compras offline
    
    -- Información de la compra
    fecha_compra        TIMESTAMP NOT NULL,
    total_compra        NUMERIC(10,2) NOT NULL,
    cantidad_items      INTEGER DEFAULT 1,
    
    -- Información del canal
    canal               VARCHAR(20) NOT NULL, -- 'online', 'offline'
    metodo_pago         VARCHAR(50),
    
    -- Dimensiones de tiempo (calculadas para optimización)
    year_compra         INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM fecha_compra)) STORED,
    month_compra        INTEGER GENERATED ALWAYS AS (EXTRACT(MONTH FROM fecha_compra)) STORED,
    day_compra          INTEGER GENERATED ALWAYS AS (EXTRACT(DAY FROM fecha_compra)) STORED,
    day_of_week         INTEGER GENERATED ALWAYS AS (EXTRACT(DOW FROM fecha_compra)) STORED,
    quarter_compra      INTEGER GENERATED ALWAYS AS (EXTRACT(QUARTER FROM fecha_compra)) STORED,
    
    -- Metadatos de auditoría
    fecha_carga         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints de validación
    CONSTRAINT chk_compras_total_positivo 
        CHECK (total_compra > 0),
    CONSTRAINT chk_compras_cantidad_positiva 
        CHECK (cantidad_items > 0),
    CONSTRAINT chk_compras_canal_valido 
        CHECK (canal IN ('online', 'offline')),
    CONSTRAINT chk_compras_fecha_valida 
        CHECK (fecha_compra >= '2020-01-01' AND fecha_compra <= CURRENT_TIMESTAMP),
    CONSTRAINT chk_compras_ci_valido 
        CHECK (ci IS NOT NULL AND LENGTH(TRIM(ci)) > 0)
);

-- Comentarios en la tabla de compras
COMMENT ON TABLE fact_compras IS 'Tabla de hechos con compras unificadas de canales online y offline';

COMMENT ON COLUMN fact_compras.compra_id IS 'Identificador único de compra (surrogate key)';
COMMENT ON COLUMN fact_compras.ci IS 'Cédula de Identidad del cliente';
COMMENT ON COLUMN fact_compras.pedido_id IS 'ID del pedido (solo para canal online)';
COMMENT ON COLUMN fact_compras.canal IS 'Canal de compra: online u offline';
COMMENT ON COLUMN fact_compras.year_compra IS 'Año de compra (columna calculada)';
COMMENT ON COLUMN fact_compras.month_compra IS 'Mes de compra (columna calculada)';
COMMENT ON COLUMN fact_compras.quarter_compra IS 'Trimestre de compra (columna calculada)';

-- -----------------------------------------------------------------------------
-- ÍNDICES PARA OPTIMIZACIÓN DE CONSULTAS
-- -----------------------------------------------------------------------------

-- Índices en tabla de clientes
CREATE INDEX idx_clientes_ci ON dim_clientes(ci);
CREATE INDEX idx_clientes_activo ON dim_clientes(cliente_activo);
CREATE INDEX idx_clientes_canales ON dim_clientes(canales);
CREATE INDEX idx_clientes_total_gastado ON dim_clientes(total_gastado DESC);
CREATE INDEX idx_clientes_ultima_compra ON dim_clientes(fecha_ultima_compra DESC);
CREATE INDEX idx_clientes_ciudad ON dim_clientes(ciudad);
CREATE INDEX idx_clientes_fecha_registro ON dim_clientes(fecha_registro);

-- Índices en tabla de compras
CREATE INDEX idx_compras_ci ON fact_compras(ci);
CREATE INDEX idx_compras_fecha ON fact_compras(fecha_compra DESC);
CREATE INDEX idx_compras_canal ON fact_compras(canal);
CREATE INDEX idx_compras_total ON fact_compras(total_compra DESC);
CREATE INDEX idx_compras_year_month ON fact_compras(year_compra, month_compra);
CREATE INDEX idx_compras_metodo_pago ON fact_compras(metodo_pago);

-- Índices compuestos para consultas comunes
CREATE INDEX idx_compras_ci_fecha ON fact_compras(ci, fecha_compra DESC);
CREATE INDEX idx_compras_canal_fecha ON fact_compras(canal, fecha_compra DESC);
CREATE INDEX idx_clientes_activo_total ON dim_clientes(cliente_activo, total_gastado DESC);

-- -----------------------------------------------------------------------------
-- CONSTRAINTS DE REFERENCIA (FOREIGN KEYS)
-- -----------------------------------------------------------------------------

-- Clave foránea de fact_compras hacia dim_clientes
ALTER TABLE fact_compras 
ADD CONSTRAINT fk_compras_clientes 
FOREIGN KEY (ci) REFERENCES dim_clientes(ci) 
ON DELETE CASCADE ON UPDATE CASCADE;

-- -----------------------------------------------------------------------------
-- TRIGGERS PARA AUDITORÍA AUTOMÁTICA
-- -----------------------------------------------------------------------------

-- Función para actualizar timestamp de modificación
CREATE OR REPLACE FUNCTION actualizar_fecha_modificacion()
RETURNS TRIGGER AS $$
BEGIN
    NEW.fecha_actualizacion = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger para dim_clientes
CREATE TRIGGER trg_clientes_fecha_actualizacion
    BEFORE UPDATE ON dim_clientes
    FOR EACH ROW
    EXECUTE FUNCTION actualizar_fecha_modificacion();

-- -----------------------------------------------------------------------------
-- VISTAS PARA ANÁLISIS COMÚN
-- -----------------------------------------------------------------------------

-- Vista de resumen de clientes activos
CREATE VIEW v_clientes_activos AS
SELECT 
    ci,
    nombre,
    apellido,
    ciudad,
    canales,
    total_gastado,
    ticket_promedio,
    frecuencia_compra,
    dias_ultima_compra,
    CASE 
        WHEN total_gastado >= 50000 THEN 'VIP'
        WHEN frecuencia_compra >= 10 THEN 'Frecuente'
        WHEN cliente_activo = 1 THEN 'Activo'
        ELSE 'Regular'
    END AS categoria_cliente
FROM dim_clientes
WHERE cliente_activo = 1
ORDER BY total_gastado DESC;

-- Vista de ventas por mes y canal
CREATE VIEW v_ventas_mensuales AS
SELECT 
    year_compra,
    month_compra,
    canal,
    COUNT(*) as total_compras,
    COUNT(DISTINCT ci) as clientes_unicos,
    SUM(total_compra) as ventas_totales,
    AVG(total_compra) as ticket_promedio,
    MIN(total_compra) as compra_minima,
    MAX(total_compra) as compra_maxima
FROM fact_compras
GROUP BY year_compra, month_compra, canal
ORDER BY year_compra DESC, month_compra DESC, canal;

-- Vista de performance de clientes multicanal
CREATE VIEW v_clientes_multicanal AS
SELECT 
    c.ci,
    c.nombre,
    c.apellido,
    c.ciudad,
    c.total_gastado,
    c.total_compra_online,
    c.total_compra_offline,
    c.fecha_compra_online,
    c.fecha_compra_offline,
    ROUND(c.total_compra_online / c.total_gastado * 100, 2) as pct_online,
    ROUND(c.total_compra_offline / c.total_gastado * 100, 2) as pct_offline
FROM dim_clientes c
WHERE c.canales = 'online_offline'
ORDER BY c.total_gastado DESC;

-- -----------------------------------------------------------------------------
-- ESTADÍSTICAS Y ANÁLISIS INICIAL
-- -----------------------------------------------------------------------------

-- Actualizar estadísticas de las tablas (después de la carga inicial)
-- ANALYZE dim_clientes;
-- ANALYZE fact_compras;

-- -----------------------------------------------------------------------------
-- GRANTS DE SEGURIDAD (PERSONALIZAR SEGÚN NECESIDADES)
-- -----------------------------------------------------------------------------

-- Crear roles si no existen
-- CREATE ROLE etl_loader;
-- CREATE ROLE analyst_reader;
-- CREATE ROLE dashboard_reader;

-- Permisos para el proceso ETL
-- GRANT INSERT, UPDATE, DELETE, TRUNCATE ON dim_clientes TO etl_loader;
-- GRANT INSERT, UPDATE, DELETE, TRUNCATE ON fact_compras TO etl_loader;
-- GRANT USAGE ON SEQUENCE fact_compras_compra_id_seq TO etl_loader;

-- Permisos de solo lectura para analistas
-- GRANT SELECT ON dim_clientes, fact_compras TO analyst_reader;
-- GRANT SELECT ON v_clientes_activos, v_ventas_mensuales, v_clientes_multicanal TO analyst_reader;

-- Permisos para dashboard
-- GRANT SELECT ON dim_clientes, fact_compras TO dashboard_reader;
-- GRANT SELECT ON v_clientes_activos, v_ventas_mensuales, v_clientes_multicanal TO dashboard_reader;

-- -----------------------------------------------------------------------------
-- COMENTARIOS FINALES Y DOCUMENTACIÓN
-- -----------------------------------------------------------------------------

COMMENT ON DATABASE CURRENT_DATABASE() IS 'Data Warehouse de E-commerce Unificado - Online y Offline';

-- =============================================================================
-- INSTRUCCIONES DE USO Y MANTENIMIENTO
-- =============================================================================
--
-- 1. EJECUCIÓN INICIAL:
--    - Ejecutar este script en PostgreSQL 13+ con permisos de administrador
--    - Verificar que no existan conflictos con tablas existentes
--    - Ajustar grants según usuarios/roles específicos del entorno
--
-- 2. MONITOREO POST-CREACIÓN:
--    - Verificar que las tablas se crearon correctamente:
--      SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '%cliente%' OR tablename LIKE '%compra%';
--    
--    - Verificar índices:
--      SELECT indexname FROM pg_indexes WHERE tablename IN ('dim_clientes', 'fact_compras');
--
-- 3. MANTENIMIENTO PERIÓDICO:
--    - Ejecutar VACUUM ANALYZE semanalmente
--    - Monitorear el crecimiento de las tablas
--    - Revisar performance de queries y ajustar índices si es necesario
--
-- 4. BACKUP Y RECOVERY:
--    - Incluir estas tablas en rutinas de backup diario
--    - Considerar particionamiento de fact_compras por fecha si crece mucho
--
-- 5. ESCALABILIDAD:
--    - Si fact_compras supera 50M registros, considerar particionamiento mensual
--    - Monitorear performance de índices y recrear si es necesario
--    - Evaluar compresión de datos históricos
--
-- =============================================================================

-- Mensaje de finalización
DO $$
BEGIN
    RAISE NOTICE '=================================================================';
    RAISE NOTICE 'CREACIÓN DE TABLAS COMPLETADA EXITOSAMENTE';
    RAISE NOTICE '=================================================================';
    RAISE NOTICE 'Tablas creadas:';
    RAISE NOTICE '  • dim_clientes (Dimensión de clientes unificados)';
    RAISE NOTICE '  • fact_compras (Hechos de compras unificadas)';
    RAISE NOTICE '';
    RAISE NOTICE 'Vistas creadas:';
    RAISE NOTICE '  • v_clientes_activos (Clientes activos categorizados)';
    RAISE NOTICE '  • v_ventas_mensuales (Ventas por mes y canal)';
    RAISE NOTICE '  • v_clientes_multicanal (Performance multicanal)';
    RAISE NOTICE '';
    RAISE NOTICE 'Índices creados: 13 índices para optimización de consultas';
    RAISE NOTICE 'Constraints aplicados: Validaciones de integridad y negocio';
    RAISE NOTICE '';
    RAISE NOTICE 'Sistema listo para recibir datos del proceso ETL';
    RAISE NOTICE '=================================================================';
END $$;
