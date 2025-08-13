# Sistema ETL E-commerce Unificado

## 📋 Descripción General

Sistema ETL profesional desarrollado en Python que unifica datos de e-commerce de múltiples canales (online y offline) en un data warehouse centralizado. El sistema extrae datos desde SQL Server (canal online) y archivos Excel (canal offline), los transforma aplicando reglas de deduplicación y cálculo de métricas, y los carga en PostgreSQL para análisis unificado.

### 🎯 Características Principales

- **Extracción Multi-fuente**: SQL Server (online) + Excel (offline)
- **Deduplicación Inteligente**: Unificación de clientes por CI con reglas configurables
- **Métricas Agregadas**: Cálculo automático de KPIs de negocio
- **Procesamiento Optimizado**: Chunking y batching para grandes volúmenes
- **Validaciones Robustas**: Controles de calidad comprehensivos
- **Logging Detallado**: Sistema de logging con rotación automática
- **Configuración Flexible**: Configuración via YAML
- **Manejo de Errores**: Reintentos automáticos y recuperación de errores
- **Modo Dry-Run**: Validaciones sin procesamiento real

## 🏗️ Arquitectura del Sistema

```
┌─────────────────┐    ┌─────────────────┐
│   SQL Server    │    │     Excel       │
│   (E-commerce   │    │   (Compras      │
│    Online)      │    │   Offline)      │
└─────────┬───────┘    └─────────┬───────┘
          │                      │
          └──────────┬───────────┘
                     │
              ┌──────▼──────┐
              │   EXTRACTOR │
              │  - SQL Data │
              │  - Excel    │
              └──────┬──────┘
                     │
              ┌──────▼──────┐
              │TRANSFORMER  │
              │- Deduplicate│
              │- Unify      │
              │- Metrics    │
              └──────┬──────┘
                     │
              ┌──────▼──────┐
              │  VALIDATOR  │
              │ - Quality   │
              │ - Business  │
              └──────┬──────┘
                     │
              ┌──────▼──────┐
              │   LOADER    │
              │ PostgreSQL  │
              └─────────────┘
```

### 🔧 Componentes Principales

| Componente | Responsabilidad |
|------------|-----------------|
| **Extractor** | Conexión y extracción desde SQL Server y Excel |
| **Transformer** | Deduplicación, unificación y cálculo de métricas |
| **Loader** | Carga optimizada hacia PostgreSQL |
| **Validator** | Validaciones de calidad y reglas de negocio |
| **Logger** | Logging centralizado con rotación |
| **ETLPipeline** | Orquestación de todo el proceso |

## 🚀 Instalación y Configuración

### 📋 Pre-requisitos

- **Python**: 3.8 o superior (recomendado 3.9+)
- **SQL Server**: Acceso de lectura a base de datos online
- **PostgreSQL**: Acceso de escritura para data warehouse
- **Excel**: Archivo con datos offline en formato específico
- **Drivers**: ODBC Driver para SQL Server

### 🔧 Instalación

1. **Clonar/Descargar archivos del proyecto**
```bash
# Crear directorio del proyecto
mkdir etl_ecommerce
cd etl_ecommerce

# Copiar todos los archivos del sistema ETL
# etl_main.py, run_etl.py, config.yaml, etc.
```

2. **Crear entorno virtual (recomendado)**
```bash
python -m venv etl_env
source etl_env/bin/activate  # Linux/Mac
# o
etl_env\Scripts\activate     # Windows
```

3. **Instalar dependencias**
```bash
pip install -r requirements.txt
```

4. **Instalar drivers de base de datos**

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install python3-dev build-essential unixodbc-dev

# Microsoft ODBC Driver para SQL Server
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

**CentOS/RHEL:**
```bash
sudo yum install python3-devel gcc unixODBC-devel

# Microsoft ODBC Driver para SQL Server
sudo curl -o /etc/yum.repos.d/mssql-release.repo https://packages.microsoft.com/config/rhel/8/prod.repo
sudo yum remove unixODBC-utf16 unixODBC-utf16-devel
sudo ACCEPT_EULA=Y yum install -y msodbcsql17
```

### ⚙️ Configuración

1. **Configurar variables de entorno**
```bash
# Crear archivo .env (opcional)
export SQL_SERVER_PASSWORD="tu_password_sql_server"
export POSTGRES_PASSWORD="tu_password_postgresql"
```

2. **Personalizar config.yaml**
```yaml
# Editar config.yaml con tus credenciales y rutas
databases:
  sql_server:
    host: "tu-sql-server.com"
    user: "tu_usuario"
    password: "${SQL_SERVER_PASSWORD}"
    database: "tu_database"
  
  postgresql:
    host: "tu-postgresql.com"
    user: "tu_usuario"
    password: "${POSTGRES_PASSWORD}"
    database: "tu_datawarehouse"

data_sources:
  excel:
    file_path: "/ruta/a/tu/archivo_compras_offline.xlsx"
```

3. **Crear estructura de base de datos**
```bash
# Ejecutar en PostgreSQL
psql -U tu_usuario -d tu_database -f create_tables.sql
```

4. **Crear directorios necesarios**
```bash
mkdir -p logs/etl
mkdir -p data/input
```

## 🎮 Uso del Sistema

### 🚀 Ejecución Básica

```bash
# Ejecución normal
python run_etl.py

# Con configuración personalizada
python run_etl.py --config mi_config.yaml

# Modo verbose (logging detallado)
python run_etl.py --verbose

# Modo silencioso (solo errores)
python run_etl.py --quiet
```

### 🔍 Modos de Validación

```bash
# Validar solo configuración
python run_etl.py --validate-config

# Modo dry-run (validaciones sin procesar datos)
python run_etl.py --dry-run

# Dry-run con logging detallado
python run_etl.py --dry-run --verbose
```

### 🛠️ Opciones Avanzadas

```bash
# Procesar solo una muestra (para pruebas)
python run_etl.py --sample-size 1000

# Saltar validaciones (no recomendado en producción)
python run_etl.py --skip-validations

# Log file personalizado
python run_etl.py --log-file mi_log_personalizado.log

# Ver todas las opciones
python run_etl.py --help
```

### 📅 Programación Automática

**Usando cron (Linux/Mac):**
```bash
# Editar crontab
crontab -e

# Ejecutar diario a las 2:00 AM
0 2 * * * cd /ruta/al/proyecto && source etl_env/bin/activate && python run_etl.py >> logs/cron.log 2>&1
```

**Usando Task Scheduler (Windows):**
1. Abrir Task Scheduler
2. Create Basic Task...
3. Configurar trigger diario a las 2:00 AM
4. Action: Start a program
   - Program: `C:\ruta\al\python.exe`
   - Arguments: `run_etl.py`
   - Start in: `C:\ruta\al\proyecto`

## 📊 Estructura de Datos

### 📥 Datos de Entrada

**SQL Server (Canal Online):**
- `clientes`: Información de clientes registrados
- `pedidos`: Órdenes de compra online
- `productos`: Catálogo de productos
- `pedido_items`: Detalle de items por pedido
- `pagos`: Información de pagos

**Excel (Canal Offline):**
```
| CI Cliente | Nombre Completo | Email | Teléfono | Ciudad | Fecha Compra | Total Compra | Cantidad Items | Método Pago |
|------------|----------------|--------|----------|--------|--------------|--------------|----------------|-------------|
```

### 📤 Datos de Salida

**dim_clientes** (Dimensión de clientes unificados):
```sql
SELECT 
    ci,                     -- Cédula de identidad (clave natural)
    nombre, apellido,       -- Información personal
    canales,               -- 'online', 'offline', 'online_offline'
    total_gastado,         -- Total gastado en ambos canales
    ticket_promedio,       -- Ticket promedio de compras
    frecuencia_compra,     -- Número total de compras
    cliente_activo,        -- 1=Activo últimos 90 días, 0=Inactivo
    dias_ultima_compra     -- Días desde última compra
FROM dim_clientes 
WHERE cliente_activo = 1
ORDER BY total_gastado DESC;
```

**fact_compras** (Hechos de compras unificadas):
```sql
SELECT 
    ci,                    -- Cédula del cliente
    fecha_compra,          -- Fecha de la compra
    total_compra,          -- Monto de la compra
    canal,                 -- 'online' o 'offline'
    year_compra,           -- Año (columna calculada)
    month_compra           -- Mes (columna calculada)
FROM fact_compras 
WHERE fecha_compra >= '2025-01-01'
ORDER BY fecha_compra DESC;
```

## 📈 Métricas y KPIs Calculados

### 👤 Métricas por Cliente

| Métrica | Descripción | Cálculo |
|---------|-------------|---------|
| **total_gastado** | Total gastado en ambos canales | SUM(total_compra) |
| **ticket_promedio** | Ticket promedio | AVG(total_compra) |
| **frecuencia_compra** | Número de compras | COUNT(compras) |
| **cliente_activo** | Compró en últimos 90 días | Fecha_última > HOY-90 |
| **dias_ultima_compra** | Días desde última compra | HOY - MAX(fecha_compra) |

### 📊 Métricas por Canal

| Métrica | Online | Offline |
|---------|--------|---------|
| **total_compra_online** | ✅ | ❌ |
| **total_compra_offline** | ❌ | ✅ |
| **fecha_compra_online** | ✅ | ❌ |
| **fecha_compra_offline** | ❌ | ✅ |

### 🏷️ Categorización de Clientes

```python
# Lógica de categorización implementada
if total_gastado >= 50000:
    categoria = 'VIP'
elif frecuencia_compra >= 10:
    categoria = 'Frecuente'  
elif cliente_activo == 1:
    categoria = 'Activo'
else:
    categoria = 'Regular'
```

## 🔍 Validaciones de Calidad

### ✅ Validaciones de Extracción

- **Mínimo de registros**: Verificar que cada tabla tenga el mínimo esperado
- **Columnas requeridas**: Validar existencia de campos críticos
- **Tipos de datos**: Verificar tipos correctos
- **Valores nulos**: Límites máximos de nulos por columna
- **Conectividad**: Test de conexiones a bases de datos

### ✅ Validaciones de Transformación

- **Unicidad**: Verificar que no hay CIs duplicados
- **Rangos válidos**: Montos positivos, fechas coherentes
- **Consistencia**: Métricas calculadas coinciden con datos origen
- **Integridad referencial**: Todos los CIs en compras existen en clientes

### ✅ Validaciones de Negocio

```yaml
# Configurables en config.yaml
validation_rules:
  min_rows:
    clientes: 100
    pedidos: 500
  
  max_null_percentage:
    ci: 0.01          # Máximo 1% nulos
    total_compra: 0.0 # 0% nulos
  
  valid_ranges:
    total_compra:
      min: 0.01
      max: 100000
```

## 📁 Estructura de Archivos

```
etl_ecommerce/
├── 📄 etl_main.py           # Script ETL principal con todas las clases
├── 🏃 run_etl.py            # Script de ejecución con CLI
├── ⚙️ config.yaml           # Configuración completa del sistema
├── 🗃️ create_tables.sql     # Script de creación de tablas PostgreSQL
├── 📋 requirements.txt      # Dependencias Python
├── 📖 README.md            # Documentación (este archivo)
├── 📁 logs/                # Directorio de logs
│   └── 📄 etl_ecommerce.log
├── 📁 data/                # Directorio de datos de entrada
│   └── 📁 input/
│       └── 📄 compras_offline.xlsx
└── 📁 etl_env/             # Entorno virtual (si se usa)
```

## 🔧 Configuración Avanzada

### 🎛️ Parámetros de Performance

```yaml
etl_settings:
  chunk_size: 10000          # Registros por chunk
  batch_size: 5000           # Registros por batch de inserción
  max_workers: 4             # Procesos paralelos
  max_retries: 3             # Reintentos por operación
  retry_delay: 2.0           # Segundos entre reintentos

performance:
  memory_limit_gb: 8         # Límite de memoria
  parallel_processing: true  # Procesamiento paralelo
  max_concurrent_connections: 5
```

### 📝 Configuración de Logging

```yaml
logging:
  level: "INFO"              # DEBUG, INFO, WARNING, ERROR, CRITICAL
  log_dir: "/logs/etl"       # Directorio de logs
  filename: "etl_ecommerce.log"
  max_size_mb: 10           # Tamaño máximo por archivo
  backup_count: 5           # Archivos de backup
  console_output: true      # Mostrar en consola
```

### 🛡️ Reglas de Negocio Configurables

```yaml
business_rules:
  dias_cliente_activo: 90    # Días para cliente activo
  
  merge_rules:               # Reglas de fusión para deduplicación
    nombre: "longer_value"   # longer_value, online_priority, offline_priority
    email: "online_priority"
    telefono: "offline_priority"
  
  cliente_vip_threshold: 50000      # Monto para cliente VIP
  cliente_frecuente_threshold: 10   # Compras para cliente frecuente
```

## 📊 Monitoreo y Logging

### 📈 Ejemplo de Output de Logs

```
2025-08-12 02:00:01 |     INFO | ETL_System | run | ================================================================================
2025-08-12 02:00:01 |     INFO | ETL_System | run | INICIANDO PIPELINE ETL - 2025-08-12 02:00:01
2025-08-12 02:00:01 |     INFO | ETL_System | run | ================================================================================
2025-08-12 02:00:02 |     INFO | ETL_System | extract_from_sql_server | Iniciando extracción desde SQL Server...
2025-08-12 02:00:05 |     INFO | ETL_System | extract_from_sql_server | Tabla clientes extraída: 45,231 registros
2025-08-12 02:00:08 |     INFO | ETL_System | extract_from_sql_server | Tabla pedidos extraída: 128,945 registros
2025-08-12 02:00:12 |     INFO | ETL_System | extract_from_excel | Extrayendo datos desde Excel: /data/input/compras_offline.xlsx
2025-08-12 02:00:13 |     INFO | ETL_System | extract_from_excel | Datos Excel extraídos: 12,456 registros
2025-08-12 02:00:15 |     INFO | ETL_System | deduplicate_clientes | Iniciando deduplicación de clientes por CI...
2025-08-12 02:00:16 |     INFO | ETL_System | deduplicate_clientes | Clientes únicos online: 45,231
2025-08-12 02:00:16 |     INFO | ETL_System | deduplicate_clientes | Clientes únicos offline: 8,234
2025-08-12 02:00:16 |     INFO | ETL_System | deduplicate_clientes | Clientes presentes en ambos canales: 3,456
2025-08-12 02:00:18 |     INFO | ETL_System | deduplicate_clientes | Deduplicación completada: 50,009 clientes únicos
2025-08-12 02:00:22 |     INFO | ETL_System | calculate_metricas_cliente | Calculando métricas agregadas por cliente...
2025-08-12 02:00:25 |     INFO | ETL_System | calculate_metricas_cliente | Métricas calculadas para 50,009 clientes
2025-08-12 02:00:25 |     INFO | ETL_System | calculate_metricas_cliente | Clientes activos: 32,145
2025-08-12 02:00:26 |     INFO | ETL_System | load_data | Iniciando carga de datos en PostgreSQL...
2025-08-12 02:00:30 |     INFO | ETL_System | load_data | Carga de datos completada exitosamente
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | ================================================================================
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | RESUMEN DE EJECUCIÓN
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | ================================================================================
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | Total clientes procesados: 50,009
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | Total compras procesadas: 141,401
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | Clientes online_offline: 3,456
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | Clientes online: 41,775
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | Clientes offline: 4,778
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | Compras online: 128,945
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | Compras offline: 12,456
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | Clientes activos (90 días): 32,145
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | Revenue total: $15,234,567.89
2025-08-12 02:00:31 |     INFO | ETL_System | _generate_summary | Ticket promedio global: $107.73
2025-08-12 02:00:31 |     INFO | ETL_System | _finalize_execution | --------------------------------------------------------------------------------
2025-08-12 02:00:31 |     INFO | ETL_System | _finalize_execution | PIPELINE ETL FINALIZADO - 2025-08-12 02:00:31
2025-08-12 02:00:31 |     INFO | ETL_System | _finalize_execution | Tiempo total de ejecución: 0:00:30.123456
2025-08-12 02:00:31 |     INFO | ETL_System | _finalize_execution | ================================================================================
```

### 📊 Métricas de Monitoreo

```sql
-- Consultas de monitoreo recomendadas

-- 1. Resumen general de clientes
SELECT 
    canales,
    COUNT(*) as total_clientes,
    SUM(CASE WHEN cliente_activo = 1 THEN 1 ELSE 0 END) as clientes_activos,
    AVG(total_gastado) as gasto_promedio,
    SUM(total_gastado) as revenue_total
FROM dim_clientes 
GROUP BY canales;

-- 2. Tendencias de compras por mes
SELECT 
    year_compra,
    month_compra,
    canal,
    COUNT(*) as compras,
    SUM(total_compra) as ventas,
    COUNT(DISTINCT ci) as clientes_unicos
FROM fact_compras 
WHERE fecha_compra >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY year_compra, month_compra, canal
ORDER BY year_compra DESC, month_compra DESC;

-- 3. Top 10 clientes por valor
SELECT 
    ci,
    nombre,
    apellido,
    canales,
    total_gastado,
    frecuencia_compra,
    dias_ultima_compra
FROM dim_clientes
ORDER BY total_gastado DESC
LIMIT 10;
```

## 🚨 Troubleshooting

### ❌ Errores Comunes y Soluciones

**1. Error de Conexión SQL Server**
```
Error: [Microsoft][ODBC Driver 17 for SQL Server]
```
**Solución:**
- Verificar que el driver ODBC esté instalado
- Comprobar conectividad de red al servidor
- Validar credenciales y permisos

**2. Error de Memoria (Pandas)**
```
MemoryError: Unable to allocate array
```
**Solución:**
```yaml
# Reducir chunk_size en config.yaml
etl_settings:
  chunk_size: 5000  # Reducir de 10000 a 5000
```

**3. Error de Archivo Excel**
```
FileNotFoundError: Excel file not found
```
**Solución:**
- Verificar que el archivo existe en la ruta especificada
- Comprobar permisos de lectura
- Validar formato del archivo (debe ser .xlsx)

**4. Error de Conexión PostgreSQL**
```
psycopg2.OperationalError: could not connect
```
**Solución:**
- Verificar que PostgreSQL esté ejecutándose
- Comprobar firewall y puertos (default: 5432)
- Validar credenciales y base de datos

### 🔧 Diagnóstico de Problemas

```bash
# 1. Verificar configuración
python run_etl.py --validate-config

# 2. Ejecutar en modo dry-run
python run_etl.py --dry-run --verbose

# 3. Probar con muestra pequeña
python run_etl.py --sample-size 100 --verbose

# 4. Verificar logs detalladamente
tail -f logs/etl/etl_ecommerce.log

# 5. Verificar dependencias
python -c "import pandas, sqlalchemy, psycopg2, pyodbc, yaml; print('✅ Todas las librerías OK')"
```

### 📧 Contacto y Soporte

Para problemas técnicos o consultas:

- **Email del equipo**: hola@sinergiadigital.com.py

### 📚 Recursos Adicionales

- **Documentación Pandas**: https://pandas.pydata.org/docs/
- **SQLAlchemy**: https://docs.sqlalchemy.org/
- **PostgreSQL**: https://www.postgresql.org/docs/
- **Microsoft SQL Server**: https://docs.microsoft.com/sql/
- **ODBC Drivers**: https://docs.microsoft.com/sql/connect/odbc/

---

## 📄 Licencia y Créditos

**Desarrollado por**: Sinergia Digital
**Fecha**: Junio 2025  
**Versión**: 1.0.3  
**Python**: 3.8+  
**Licencia**: Propiedad de la empresa

### 🤝 Contribuciones

Para contribuir al desarrollo:
1. Fork del repositorio
2. Crear branch de feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -am 'Agregar nueva funcionalidad'`)
4. Push al branch (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request

### 📝 Changelog

**v1.0.3** - Junio 2025
- ✨ Versión inicial del sistema ETL
- 🔄 Extracción desde SQL Server y Excel
- 🔀 Deduplicación por CI con reglas configurables
- 📊 Cálculo de métricas agregadas completas
- ✅ Validaciones comprehensivas de calidad
- 📝 Logging detallado con rotación
- 🚀 Modo dry-run para pruebas
- 📋 CLI completa con múltiples opciones

---

> **💡 Tip**: Para un rendimiento óptimo en producción, programe la ejecución durante horarios de baja actividad y monitoree regularmente los logs para detectar posibles issues.

¡El sistema está listo para unificar sus datos de e-commerce! 🎉