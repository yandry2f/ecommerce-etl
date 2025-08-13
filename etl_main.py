
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sistema ETL Completo para Unificación de Datos de E-commerce
===========================================================

Autor: Sinergia Digital
Fecha: Junio 2025
Versión: 1.0.3

Descripción:
    Sistema ETL profesional que extrae datos de SQL Server (online) y Excel (offline),
    los transforma unificando clientes y calculando métricas agregadas,
    y los carga en PostgreSQL para análisis unificado.

Arquitectura:
    - Extractor: Maneja extracción de SQL Server y Excel
    - Transformer: Procesa deduplicación, unificación y métricas
    - Loader: Carga datos en PostgreSQL
    - Validator: Validaciones de calidad de datos
    - Logger: Logging centralizado con rotación
"""

import os
import sys
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from logging.handlers import RotatingFileHandler
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import pyodbc
import psycopg2
from pathlib import Path
import time
from functools import wraps
import warnings

# Suprimir warnings de pandas
warnings.filterwarnings('ignore')


class ETLLogger:
    """
    Gestor centralizado de logging con rotación de archivos
    Proporciona logging detallado para todas las operaciones del ETL
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Inicializa el sistema de logging
        
        Args:
            config: Diccionario de configuración con parámetros de logging
        """
        self.config = config.get('logging', {})
        self.logger = logging.getLogger('ETL_System')
        self.setup_logging()
    
    def setup_logging(self) -> None:
        """Configura los handlers de logging con rotación"""
        self.logger.setLevel(getattr(logging, self.config.get('level', 'INFO')))
        
        # Crear directorio de logs si no existe
        log_dir = Path(self.config.get('log_dir', 'logs'))
        log_dir.mkdir(exist_ok=True)
        
        # Formatter detallado
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para archivo con rotación
        file_handler = RotatingFileHandler(
            log_dir / self.config.get('filename', 'etl_system.log'),
            maxBytes=self.config.get('max_size_mb', 10) * 1024 * 1024,
            backupCount=self.config.get('backup_count', 5),
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        
        # Handler para consola
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        # Agregar handlers
        self.logger.addHandler(file_handler)
        if self.config.get('console_output', True):
            self.logger.addHandler(console_handler)
    
    def get_logger(self) -> logging.Logger:
        """Retorna la instancia del logger configurado"""
        return self.logger


def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """
    Decorador para reintentos automáticos en caso de fallo
    
    Args:
        max_retries: Número máximo de reintentos
        delay: Delay entre reintentos en segundos
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger('ETL_System')
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(f"Función {func.__name__} falló después de {max_retries} reintentos: {str(e)}")
                        raise
                    else:
                        logger.warning(f"Intento {attempt + 1} de {func.__name__} falló: {str(e)}. Reintentando en {delay}s...")
                        time.sleep(delay)
            return None
        return wrapper
    return decorator


class Extractor:
    """
    Extractor de datos desde múltiples fuentes:
    - SQL Server (datos de e-commerce online)
    - Excel (datos de compras offline)
    """
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Inicializa el extractor con configuración de conexiones
        
        Args:
            config: Configuración de conexiones y rutas
            logger: Logger para registrar operaciones
        """
        self.config = config
        self.logger = logger
        self.sql_server_engine = None
        
    @retry_on_failure(max_retries=3)
    def connect_sql_server(self) -> sa.engine.Engine:
        """
        Establece conexión con SQL Server
        
        Returns:
            Engine de SQLAlchemy para SQL Server
            
        Raises:
            Exception: Si no puede establecer la conexión
        """
        if self.sql_server_engine is None:
            sql_config = self.config['databases']['sql_server']
            
            # Construir connection string
            conn_str = (
                f"mssql+pyodbc://{sql_config['user']}:{sql_config['password']}"
                f"@{sql_config['host']}:{sql_config['port']}/{sql_config['database']}"
                f"?driver={sql_config['driver']}&TrustServerCertificate=yes"
            )
            
            self.sql_server_engine = create_engine(
                conn_str,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Test de conexión
            with self.sql_server_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info("Conexión a SQL Server establecida exitosamente")
        
        return self.sql_server_engine
    
    @retry_on_failure(max_retries=2)
    def extract_from_sql_server(self) -> Dict[str, pd.DataFrame]:
        """
        Extrae todas las tablas necesarias desde SQL Server
        
        Returns:
            Diccionario con DataFrames de cada tabla
        """
        self.logger.info("Iniciando extracción desde SQL Server...")
        
        engine = self.connect_sql_server()
        tables_data = {}
        
        # Definir queries para cada tabla
        queries = {
            'clientes': """
                SELECT 
                    cliente_id, ci, nombre, apellido, email, telefono, 
                    direccion, ciudad, fecha_registro, fecha_ultima_compra
                FROM clientes 
                WHERE ci IS NOT NULL AND ci != ''
            """,
            'pedidos': """
                SELECT 
                    pedido_id, cliente_id, fecha_pedido, estado, 
                    total_pedido, descuento, impuestos, metodo_envio
                FROM pedidos 
                WHERE estado IN ('completado', 'entregado')
            """,
            'productos': """
                SELECT 
                    producto_id, nombre, categoria, precio_unitario, 
                    costo_unitario, stock_actual, activo
                FROM productos
                WHERE activo = 1
            """,
            'pedido_items': """
                SELECT 
                    pedido_item_id, pedido_id, producto_id, cantidad, 
                    precio_unitario, descuento_item, total_item
                FROM pedido_items
            """,
            'pagos': """
                SELECT 
                    pago_id, pedido_id, metodo_pago, monto_pago, 
                    fecha_pago, estado_pago, referencia_transaccion
                FROM pagos
                WHERE estado_pago = 'completado'
            """
        }
        
        # Extraer cada tabla
        for table_name, query in queries.items():
            try:
                self.logger.info(f"Extrayendo tabla: {table_name}")
                
                # Usar chunking para tablas grandes
                chunk_size = self.config['etl_settings'].get('chunk_size', 10000)
                
                df = pd.read_sql_query(
                    query, 
                    engine, 
                    chunksize=chunk_size if table_name in ['pedido_items', 'pagos'] else None
                )
                
                # Si es chunked, combinar chunks
                if hasattr(df, '__iter__') and not isinstance(df, pd.DataFrame):
                    df = pd.concat(df, ignore_index=True)
                
                tables_data[table_name] = df
                self.logger.info(f"Tabla {table_name} extraída: {len(df):,} registros")
                
            except Exception as e:
                self.logger.error(f"Error extrayendo tabla {table_name}: {str(e)}")
                raise
        
        self.logger.info(f"Extracción SQL Server completada: {sum(len(df) for df in tables_data.values()):,} registros totales")
        return tables_data
    
    @retry_on_failure(max_retries=2)
    def extract_from_excel(self) -> pd.DataFrame:
        """
        Extrae datos de compras offline desde archivo Excel
        
        Returns:
            DataFrame con datos de compras offline
        """
        excel_config = self.config['data_sources']['excel']
        file_path = Path(excel_config['file_path'])
        
        self.logger.info(f"Extrayendo datos desde Excel: {file_path}")
        
        if not file_path.exists():
            raise FileNotFoundError(f"Archivo Excel no encontrado: {file_path}")
        
        # Leer Excel con configuraciones específicas
        df = pd.read_excel(
            file_path,
            sheet_name=excel_config.get('sheet_name', 0),
            header=excel_config.get('header_row', 0)
        )
        
        # Mapear columnas según configuración
        column_mapping = excel_config.get('column_mapping', {})
        if column_mapping:
            df = df.rename(columns=column_mapping)
        
        # Filtrar filas válidas
        df = df.dropna(subset=['ci', 'total_compra'])
        
        self.logger.info(f"Datos Excel extraídos: {len(df):,} registros")
        return df


class Transformer:
    """
    Transformador de datos que maneja:
    - Deduplicación de clientes por CI
    - Unificación de compras online/offline
    - Cálculo de métricas agregadas
    """
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Inicializa el transformador
        
        Args:
            config: Configuración de transformaciones
            logger: Logger para registrar operaciones
        """
        self.config = config
        self.logger = logger
        self.business_rules = config.get('business_rules', {})
    
    def deduplicate_clientes(self, clientes_online: pd.DataFrame, compras_offline: pd.DataFrame) -> pd.DataFrame:
        """
        Deduplica clientes por CI fusionando información de ambos canales
        
        Args:
            clientes_online: DataFrame de clientes del canal online
            compras_offline: DataFrame de compras offline
            
        Returns:
            DataFrame de clientes unificados y deduplicados
        """
        self.logger.info("Iniciando deduplicación de clientes por CI...")
        
        # Extraer información única de clientes offline
        clientes_offline = compras_offline.groupby('ci').agg({
            'nombre_cliente': 'first',
            'email': 'first',
            'telefono': 'first',
            'ciudad': 'first',
            'fecha_compra': ['min', 'max']
        }).reset_index()
        
        # Aplanar columnas multi-level
        clientes_offline.columns = ['ci', 'nombre', 'email', 'telefono', 'ciudad', 'primera_compra_offline', 'ultima_compra_offline']
        
        # Preparar clientes online
        clientes_online_prep = clientes_online.copy()
        clientes_online_prep['canal_origen'] = 'online'
        clientes_offline['canal_origen'] = 'offline'
        
        # Identificar clientes presentes en ambos canales
        ci_online = set(clientes_online_prep['ci'].values)
        ci_offline = set(clientes_offline['ci'].values)
        ci_comunes = ci_online.intersection(ci_offline)
        
        self.logger.info(f"Clientes únicos online: {len(ci_online):,}")
        self.logger.info(f"Clientes únicos offline: {len(ci_offline):,}")
        self.logger.info(f"Clientes presentes en ambos canales: {len(ci_comunes):,}")
        
        # Lista para almacenar clientes unificados
        clientes_unificados = []
        
        # Procesar clientes comunes (fusionar información)
        for ci in ci_comunes:
            online_data = clientes_online_prep[clientes_online_prep['ci'] == ci].iloc[0]
            offline_data = clientes_offline[clientes_offline['ci'] == ci].iloc[0]
            
            # Reglas de fusión configurables
            cliente_unificado = {
                'ci': ci,
                'cliente_id': online_data['cliente_id'],  # Mantener ID online
                'nombre': self._merge_field(online_data['nombre'], offline_data['nombre'], 'nombre'),
                'apellido': online_data.get('apellido', ''),
                'email': self._merge_field(online_data['email'], offline_data['email'], 'email'),
                'telefono': self._merge_field(online_data['telefono'], offline_data['telefono'], 'telefono'),
                'direccion': online_data.get('direccion', ''),
                'ciudad': self._merge_field(online_data['ciudad'], offline_data['ciudad'], 'ciudad'),
                'fecha_registro': online_data['fecha_registro'],
                'fecha_ultima_compra': max(
                    pd.to_datetime(online_data['fecha_ultima_compra']) if pd.notna(online_data['fecha_ultima_compra']) else pd.Timestamp.min,
                    pd.to_datetime(offline_data['ultima_compra_offline']) if pd.notna(offline_data['ultima_compra_offline']) else pd.Timestamp.min
                ),
                'canales': 'online_offline',
                'primera_compra': min(
                    pd.to_datetime(online_data['fecha_registro']),
                    pd.to_datetime(offline_data['primera_compra_offline'])
                )
            }
            clientes_unificados.append(cliente_unificado)
        
        # Agregar clientes solo online
        for _, cliente in clientes_online_prep[~clientes_online_prep['ci'].isin(ci_comunes)].iterrows():
            cliente_dict = cliente.to_dict()
            cliente_dict['canales'] = 'online'
            cliente_dict['primera_compra'] = pd.to_datetime(cliente['fecha_registro'])
            clientes_unificados.append(cliente_dict)
        
        # Agregar clientes solo offline (generar IDs)
        max_cliente_id = clientes_online_prep['cliente_id'].max()
        for i, (_, cliente) in enumerate(clientes_offline[~clientes_offline['ci'].isin(ci_comunes)].iterrows()):
            cliente_unificado = {
                'ci': cliente['ci'],
                'cliente_id': max_cliente_id + i + 1,
                'nombre': cliente['nombre'],
                'apellido': '',
                'email': cliente['email'],
                'telefono': cliente['telefono'],
                'direccion': '',
                'ciudad': cliente['ciudad'],
                'fecha_registro': cliente['primera_compra_offline'],
                'fecha_ultima_compra': cliente['ultima_compra_offline'],
                'canales': 'offline',
                'primera_compra': pd.to_datetime(cliente['primera_compra_offline'])
            }
            clientes_unificados.append(cliente_unificado)
        
        df_unificado = pd.DataFrame(clientes_unificados)
        
        self.logger.info(f"Deduplicación completada: {len(df_unificado):,} clientes únicos")
        return df_unificado
    
    def _merge_field(self, online_value: Any, offline_value: Any, field_type: str) -> Any:
        """
        Aplica reglas de fusión para campos específicos
        
        Args:
            online_value: Valor del canal online
            offline_value: Valor del canal offline
            field_type: Tipo de campo para aplicar regla específica
            
        Returns:
            Valor fusionado según reglas de negocio
        """
        merge_rules = self.business_rules.get('merge_rules', {})
        
        # Si uno de los valores está vacío/nulo, usar el otro
        if pd.isna(online_value) or online_value == '':
            return offline_value
        if pd.isna(offline_value) or offline_value == '':
            return online_value
        
        # Reglas específicas por tipo de campo
        rule = merge_rules.get(field_type, 'online_priority')
        
        if rule == 'online_priority':
            return online_value
        elif rule == 'offline_priority':
            return offline_value
        elif rule == 'longer_value':
            return online_value if len(str(online_value)) >= len(str(offline_value)) else offline_value
        elif rule == 'most_recent':
            # Para este ejemplo, prioritizar online como más reciente
            return online_value
        
        return online_value
    
    def unify_compras(self, sql_data: Dict[str, pd.DataFrame], excel_data: pd.DataFrame) -> pd.DataFrame:
        """
        Unifica compras de ambos canales por cliente
        
        Args:
            sql_data: Datos extraídos de SQL Server
            excel_data: Datos extraídos de Excel
            
        Returns:
            DataFrame de compras unificadas
        """
        self.logger.info("Unificando compras de ambos canales...")
        
        # Procesar compras online
        compras_online = self._process_compras_online(sql_data)
        
        # Procesar compras offline
        compras_offline = self._process_compras_offline(excel_data)
        
        # Unir ambas fuentes
        compras_unificadas = pd.concat([compras_online, compras_offline], ignore_index=True)
        
        # Ordenar por cliente y fecha
        compras_unificadas = compras_unificadas.sort_values(['ci', 'fecha_compra'])
        
        self.logger.info(f"Compras unificadas: {len(compras_unificadas):,} registros")
        self.logger.info(f"Compras online: {len(compras_online):,}, Compras offline: {len(compras_offline):,}")
        
        return compras_unificadas
    
    def _process_compras_online(self, sql_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Procesa y estructura compras del canal online"""
        # Unir tablas relacionadas
        pedidos = sql_data['pedidos']
        clientes = sql_data['clientes']
        pedido_items = sql_data['pedido_items']
        productos = sql_data['productos']
        pagos = sql_data['pagos']
        
        # Join principal: pedidos + clientes
        compras = pedidos.merge(clientes[['cliente_id', 'ci']], on='cliente_id', how='inner')
        
        # Agregar información de items
        items_agg = pedido_items.groupby('pedido_id').agg({
            'cantidad': 'sum',
            'total_item': 'sum'
        }).reset_index()
        
        compras = compras.merge(items_agg, on='pedido_id', how='left')
        
        # Estructurar para formato unificado
        compras_online = pd.DataFrame({
            'ci': compras['ci'],
            'fecha_compra': pd.to_datetime(compras['fecha_pedido']),
            'total_compra': compras['total_pedido'],
            'cantidad_items': compras['cantidad'].fillna(0),
            'canal': 'online',
            'pedido_id': compras['pedido_id'],
            'metodo_pago': compras['metodo_envio']  # Se puede mejorar con join a pagos
        })
        
        return compras_online
    
    def _process_compras_offline(self, excel_data: pd.DataFrame) -> pd.DataFrame:
        """Procesa y estructura compras del canal offline"""
        compras_offline = excel_data.copy()
        
        # Estandarizar columnas
        compras_offline['fecha_compra'] = pd.to_datetime(compras_offline['fecha_compra'])
        compras_offline['canal'] = 'offline'
        compras_offline['pedido_id'] = None
        compras_offline['cantidad_items'] = compras_offline.get('cantidad_items', 1)
        compras_offline['metodo_pago'] = compras_offline.get('metodo_pago', 'efectivo')
        
        return compras_offline[['ci', 'fecha_compra', 'total_compra', 'cantidad_items', 'canal', 'pedido_id', 'metodo_pago']]
    
    def calculate_metricas_cliente(self, clientes_unificados: pd.DataFrame, compras_unificadas: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula métricas agregadas por cliente
        
        Args:
            clientes_unificados: DataFrame de clientes deduplicados
            compras_unificadas: DataFrame de compras unificadas
            
        Returns:
            DataFrame de clientes con métricas calculadas
        """
        self.logger.info("Calculando métricas agregadas por cliente...")
        
        # Fecha de corte para cliente activo (90 días)
        fecha_corte = datetime.now() - timedelta(days=self.business_rules.get('dias_cliente_activo', 90))
        
        # Calcular métricas por cliente
        metricas = compras_unificadas.groupby('ci').agg({
            'total_compra': ['sum', 'mean', 'count'],
            'fecha_compra': ['min', 'max'],
            'canal': lambda x: len(set(x))
        }).reset_index()
        
        # Aplanar columnas multi-level
        metricas.columns = ['ci', 'total_gastado', 'ticket_promedio', 'frecuencia_compra', 
                           'primera_compra_calc', 'ultima_compra_calc', 'canales_utilizados']
        
        # Calcular cliente activo
        metricas['cliente_activo'] = (pd.to_datetime(metricas['ultima_compra_calc']) >= fecha_corte).astype(int)
        
        # Calcular días desde última compra
        metricas['dias_ultima_compra'] = (datetime.now() - pd.to_datetime(metricas['ultima_compra_calc'])).dt.days
        
        # Métricas por canal
        metricas_canal = compras_unificadas.groupby(['ci', 'canal']).agg({
            'total_compra': 'sum',
            'fecha_compra': 'count'
        }).reset_index()
        
        metricas_canal_pivot = metricas_canal.pivot(index='ci', columns='canal', values=['total_compra', 'fecha_compra']).fillna(0)
        metricas_canal_pivot.columns = [f"{stat}_{canal}" for stat, canal in metricas_canal_pivot.columns]
        metricas_canal_pivot = metricas_canal_pivot.reset_index()
        
        # Unir métricas con datos de clientes
        clientes_finales = clientes_unificados.merge(metricas, on='ci', how='left')
        clientes_finales = clientes_finales.merge(metricas_canal_pivot, on='ci', how='left')
        
        # Llenar valores NaN para clientes sin compras
        numeric_cols = ['total_gastado', 'ticket_promedio', 'frecuencia_compra', 'cliente_activo', 'dias_ultima_compra']
        for col in numeric_cols:
            if col in clientes_finales.columns:
                clientes_finales[col] = clientes_finales[col].fillna(0)
        
        self.logger.info(f"Métricas calculadas para {len(clientes_finales):,} clientes")
        self.logger.info(f"Clientes activos: {clientes_finales['cliente_activo'].sum():,}")
        
        return clientes_finales


class Loader:
    """
    Cargador de datos hacia PostgreSQL
    Maneja la carga eficiente de grandes volúmenes de datos
    """
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Inicializa el cargador
        
        Args:
            config: Configuración de conexión PostgreSQL
            logger: Logger para registrar operaciones
        """
        self.config = config
        self.logger = logger
        self.pg_engine = None
    
    @retry_on_failure(max_retries=3)
    def connect_postgresql(self) -> sa.engine.Engine:
        """
        Establece conexión con PostgreSQL
        
        Returns:
            Engine de SQLAlchemy para PostgreSQL
        """
        if self.pg_engine is None:
            pg_config = self.config['databases']['postgresql']
            
            # Construir connection string
            conn_str = (
                f"postgresql://{pg_config['user']}:{pg_config['password']}"
                f"@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
            )
            
            self.pg_engine = create_engine(
                conn_str,
                pool_pre_ping=True,
                pool_recycle=3600,
                pool_size=10,
                max_overflow=20
            )
            
            # Test de conexión
            with self.pg_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info("Conexión a PostgreSQL establecida exitosamente")
        
        return self.pg_engine
    
    @retry_on_failure(max_retries=2)
    def load_data(self, clientes_finales: pd.DataFrame, compras_unificadas: pd.DataFrame) -> None:
        """
        Carga datos transformados en PostgreSQL usando estrategia de truncate/insert
        
        Args:
            clientes_finales: DataFrame de clientes con métricas
            compras_unificadas: DataFrame de compras unificadas
        """
        self.logger.info("Iniciando carga de datos en PostgreSQL...")
        
        engine = self.connect_postgresql()
        
        # Configuraciones de carga
        chunk_size = self.config['etl_settings'].get('chunk_size', 5000)
        
        with engine.begin() as conn:
            try:
                # Truncar tablas existentes
                self.logger.info("Truncando tablas existentes...")
                conn.execute(text("TRUNCATE TABLE dim_clientes CASCADE"))
                conn.execute(text("TRUNCATE TABLE fact_compras CASCADE"))
                
                # Cargar clientes
                self.logger.info(f"Cargando {len(clientes_finales):,} clientes...")
                self._load_table_chunked(clientes_finales, 'dim_clientes', engine, chunk_size)
                
                # Cargar compras
                self.logger.info(f"Cargando {len(compras_unificadas):,} compras...")
                self._load_table_chunked(compras_unificadas, 'fact_compras', engine, chunk_size)
                
                # Actualizar estadísticas
                conn.execute(text("ANALYZE dim_clientes"))
                conn.execute(text("ANALYZE fact_compras"))
                
                self.logger.info("Carga de datos completada exitosamente")
                
            except Exception as e:
                self.logger.error(f"Error durante la carga: {str(e)}")
                raise
    
    def _load_table_chunked(self, df: pd.DataFrame, table_name: str, engine: sa.engine.Engine, chunk_size: int) -> None:
        """
        Carga DataFrame en chunks para optimizar memoria
        
        Args:
            df: DataFrame a cargar
            table_name: Nombre de la tabla destino
            engine: Engine de PostgreSQL
            chunk_size: Tamaño de cada chunk
        """
        total_chunks = (len(df) - 1) // chunk_size + 1
        
        for i, chunk in enumerate(range(0, len(df), chunk_size)):
            chunk_df = df.iloc[chunk:chunk + chunk_size]
            
            chunk_df.to_sql(
                table_name,
                engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            self.logger.info(f"Chunk {i + 1}/{total_chunks} cargado ({len(chunk_df):,} registros)")


class Validator:
    """
    Validador de calidad de datos
    Implementa controles de calidad comprehensivos
    """
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Inicializa el validador
        
        Args:
            config: Configuración de validaciones
            logger: Logger para registrar resultados
        """
        self.config = config
        self.logger = logger
        self.validation_rules = config.get('validation_rules', {})
    
    def validate_extraction(self, sql_data: Dict[str, pd.DataFrame], excel_data: pd.DataFrame) -> bool:
        """
        Valida datos extraídos
        
        Args:
            sql_data: Datos de SQL Server
            excel_data: Datos de Excel
            
        Returns:
            True si todas las validaciones pasan
        """
        self.logger.info("Iniciando validaciones de extracción...")
        
        validations_passed = True
        
        # Validar SQL Server data
        for table_name, df in sql_data.items():
            if not self._validate_table_basic(df, table_name):
                validations_passed = False
        
        # Validar Excel data
        if not self._validate_excel_basic(excel_data):
            validations_passed = False
        
        self.logger.info(f"Validaciones de extracción: {'APROBADAS' if validations_passed else 'FALLIDAS'}")
        return validations_passed
    
    def validate_transformation(self, clientes_finales: pd.DataFrame, compras_unificadas: pd.DataFrame) -> bool:
        """
        Valida datos transformados
        
        Args:
            clientes_finales: Clientes con métricas
            compras_unificadas: Compras unificadas
            
        Returns:
            True si todas las validaciones pasan
        """
        self.logger.info("Iniciando validaciones de transformación...")
        
        validations_passed = True
        
        # Validar clientes
        if not self._validate_clientes_finales(clientes_finales):
            validations_passed = False
        
        # Validar compras
        if not self._validate_compras_unificadas(compras_unificadas):
            validations_passed = False
        
        # Validar consistencia
        if not self._validate_consistency(clientes_finales, compras_unificadas):
            validations_passed = False
        
        self.logger.info(f"Validaciones de transformación: {'APROBADAS' if validations_passed else 'FALLIDAS'}")
        return validations_passed
    
    def _validate_table_basic(self, df: pd.DataFrame, table_name: str) -> bool:
        """Validaciones básicas para tablas extraídas"""
        min_rows = self.validation_rules.get('min_rows', {}).get(table_name, 0)
        
        if len(df) < min_rows:
            self.logger.error(f"Tabla {table_name}: muy pocos registros ({len(df)} < {min_rows})")
            return False
        
        if df.empty:
            self.logger.error(f"Tabla {table_name}: está vacía")
            return False
        
        self.logger.info(f"Tabla {table_name}: validación básica aprobada ({len(df):,} registros)")
        return True
    
    def _validate_excel_basic(self, df: pd.DataFrame) -> bool:
        """Validaciones básicas para datos Excel"""
        required_columns = ['ci', 'fecha_compra', 'total_compra']
        
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            self.logger.error(f"Excel: columnas faltantes: {missing_cols}")
            return False
        
        if df['ci'].isna().sum() > len(df) * 0.05:  # Máximo 5% de CIs nulos
            self.logger.error(f"Excel: demasiados CIs nulos ({df['ci'].isna().sum()}/{len(df)})")
            return False
        
        self.logger.info(f"Excel: validación básica aprobada ({len(df):,} registros)")
        return True
    
    def _validate_clientes_finales(self, df: pd.DataFrame) -> bool:
        """Validaciones para clientes finales"""
        # Verificar unicidad de CI
        duplicated_ci = df['ci'].duplicated().sum()
        if duplicated_ci > 0:
            self.logger.error(f"Clientes: {duplicated_ci} CIs duplicados encontrados")
            return False
        
        # Verificar campos requeridos
        required_fields = ['ci', 'nombre', 'total_gastado', 'ticket_promedio', 'frecuencia_compra']
        for field in required_fields:
            if field in df.columns:
                null_count = df[field].isna().sum()
                if null_count > 0:
                    self.logger.warning(f"Clientes: {null_count} valores nulos en {field}")
        
        # Verificar rangos lógicos
        if (df['total_gastado'] < 0).any():
            self.logger.error("Clientes: valores negativos en total_gastado")
            return False
        
        self.logger.info(f"Clientes finales: validación aprobada ({len(df):,} clientes únicos)")
        return True
    
    def _validate_compras_unificadas(self, df: pd.DataFrame) -> bool:
        """Validaciones para compras unificadas"""
        # Verificar valores positivos
        if (df['total_compra'] <= 0).any():
            negative_count = (df['total_compra'] <= 0).sum()
            self.logger.error(f"Compras: {negative_count} valores no positivos en total_compra")
            return False
        
        # Verificar fechas válidas
        invalid_dates = pd.to_datetime(df['fecha_compra'], errors='coerce').isna().sum()
        if invalid_dates > 0:
            self.logger.error(f"Compras: {invalid_dates} fechas inválidas")
            return False
        
        self.logger.info(f"Compras unificadas: validación aprobada ({len(df):,} compras)")
        return True
    
    def _validate_consistency(self, clientes: pd.DataFrame, compras: pd.DataFrame) -> bool:
        """Validaciones de consistencia entre tablas"""
        # Verificar que todos los CIs en compras existan en clientes
        ci_compras = set(compras['ci'].unique())
        ci_clientes = set(clientes['ci'].unique())
        
        ci_faltantes = ci_compras - ci_clientes
        if ci_faltantes:
            self.logger.error(f"Consistencia: {len(ci_faltantes)} CIs en compras no existen en clientes")
            return False
        
        # Verificar coherencia de métricas
        metricas_calc = compras.groupby('ci')['total_compra'].sum().to_dict()
        
        discrepancias = 0
        for _, cliente in clientes.iterrows():
            ci = cliente['ci']
            total_esperado = metricas_calc.get(ci, 0)
            total_calculado = cliente['total_gastado']
            
            if abs(total_esperado - total_calculado) > 0.01:  # Tolerancia de 1 centavo
                discrepancias += 1
        
        if discrepancias > 0:
            self.logger.warning(f"Consistencia: {discrepancias} discrepancias en total_gastado")
        
        self.logger.info("Validaciones de consistencia aprobadas")
        return True


class ETLPipeline:
    """
    Orquestador principal del pipeline ETL
    Coordina todas las fases del proceso
    """
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Inicializa el pipeline ETL
        
        Args:
            config_path: Ruta al archivo de configuración
        """
        self.config = self._load_config(config_path)
        self.logger_manager = ETLLogger(self.config)
        self.logger = self.logger_manager.get_logger()
        
        # Inicializar componentes
        self.extractor = Extractor(self.config, self.logger)
        self.transformer = Transformer(self.config, self.logger)
        self.loader = Loader(self.config, self.logger)
        self.validator = Validator(self.config, self.logger)
        
        self.start_time = None
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Carga configuración desde archivo YAML"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            print(f"Error cargando configuración: {e}")
            raise
    
    def run(self) -> bool:
        """
        Ejecuta el pipeline ETL completo
        
        Returns:
            True si el proceso se ejecuta exitosamente
        """
        self.start_time = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info(f"INICIANDO PIPELINE ETL - {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 80)
        
        try:
            # Fase 1: Extracción
            sql_data, excel_data = self._extraction_phase()
            
            # Fase 2: Validación de extracción
            if not self.validator.validate_extraction(sql_data, excel_data):
                raise ValueError("Validaciones de extracción fallaron")
            
            # Fase 3: Transformación
            clientes_finales, compras_unificadas = self._transformation_phase(sql_data, excel_data)
            
            # Fase 4: Validación de transformación
            if not self.validator.validate_transformation(clientes_finales, compras_unificadas):
                raise ValueError("Validaciones de transformación fallaron")
            
            # Fase 5: Carga
            self._loading_phase(clientes_finales, compras_unificadas)
            
            # Fase 6: Resumen final
            self._generate_summary(clientes_finales, compras_unificadas)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error crítico en pipeline ETL: {str(e)}")
            return False
        
        finally:
            self._finalize_execution()
    
    def _extraction_phase(self) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
        """Fase de extracción de datos"""
        self.logger.info("FASE 1: EXTRACCIÓN DE DATOS")
        self.logger.info("-" * 40)
        
        # Extraer desde SQL Server
        sql_data = self.extractor.extract_from_sql_server()
        
        # Extraer desde Excel
        excel_data = self.extractor.extract_from_excel()
        
        self.logger.info("Fase de extracción completada exitosamente")
        return sql_data, excel_data
    
    def _transformation_phase(self, sql_data: Dict[str, pd.DataFrame], excel_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Fase de transformación de datos"""
        self.logger.info("\nFASE 2: TRANSFORMACIÓN DE DATOS")
        self.logger.info("-" * 40)
        
        # Deduplicar clientes
        clientes_unificados = self.transformer.deduplicate_clientes(sql_data['clientes'], excel_data)
        
        # Unificar compras
        compras_unificadas = self.transformer.unify_compras(sql_data, excel_data)
        
        # Calcular métricas
        clientes_finales = self.transformer.calculate_metricas_cliente(clientes_unificados, compras_unificadas)
        
        self.logger.info("Fase de transformación completada exitosamente")
        return clientes_finales, compras_unificadas
    
    def _loading_phase(self, clientes_finales: pd.DataFrame, compras_unificadas: pd.DataFrame) -> None:
        """Fase de carga de datos"""
        self.logger.info("\nFASE 3: CARGA DE DATOS")
        self.logger.info("-" * 40)
        
        self.loader.load_data(clientes_finales, compras_unificadas)
        
        self.logger.info("Fase de carga completada exitosamente")
    
    def _generate_summary(self, clientes_finales: pd.DataFrame, compras_unificadas: pd.DataFrame) -> None:
        """Genera resumen de ejecución"""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("RESUMEN DE EJECUCIÓN")
        self.logger.info("=" * 80)
        
        # Estadísticas generales
        self.logger.info(f"Total clientes procesados: {len(clientes_finales):,}")
        self.logger.info(f"Total compras procesadas: {len(compras_unificadas):,}")
        
        # Análisis por canal
        canales_stats = clientes_finales['canales'].value_counts()
        for canal, count in canales_stats.items():
            self.logger.info(f"Clientes {canal}: {count:,}")
        
        compras_stats = compras_unificadas['canal'].value_counts()
        for canal, count in compras_stats.items():
            self.logger.info(f"Compras {canal}: {count:,}")
        
        # Métricas de negocio
        clientes_activos = clientes_finales['cliente_activo'].sum()
        self.logger.info(f"Clientes activos (90 días): {clientes_activos:,}")
        
        total_revenue = clientes_finales['total_gastado'].sum()
        self.logger.info(f"Revenue total: ${total_revenue:,.2f}")
        
        ticket_promedio_global = compras_unificadas['total_compra'].mean()
        self.logger.info(f"Ticket promedio global: ${ticket_promedio_global:.2f}")
    
    def _finalize_execution(self) -> None:
        """Finaliza la ejecución registrando tiempo total"""
        if self.start_time:
            end_time = datetime.now()
            duration = end_time - self.start_time
            
            self.logger.info("-" * 80)
            self.logger.info(f"PIPELINE ETL FINALIZADO - {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"Tiempo total de ejecución: {duration}")
            self.logger.info("=" * 80)


if __name__ == "__main__":
    """
    Punto de entrada principal
    Ejecuta el pipeline ETL con manejo de errores
    """
    try:
        # Inicializar y ejecutar pipeline
        pipeline = ETLPipeline()
        success = pipeline.run()
        
        if success:
            print("✅ Pipeline ETL ejecutado exitosamente")
            sys.exit(0)
        else:
            print("❌ Pipeline ETL falló")
            sys.exit(1)
            
    except Exception as e:
        print(f"💥 Error crítico: {e}")
        sys.exit(1)
