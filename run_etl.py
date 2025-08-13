
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de Ejecución del Sistema ETL E-commerce Unificado
========================================================

Este script proporciona una interfaz de línea de comandos para ejecutar
el sistema ETL con diferentes opciones de configuración y modos de ejecución.

Características:
- Ejecución con diferentes configuraciones
- Modo dry-run para pruebas
- Logging personalizado
- Validación de pre-requisitos
- Manejo de argumentos de línea de comandos

Uso:
    python run_etl.py [opciones]

Ejemplos:
    # Ejecución normal
    python run_etl.py
    
    # Ejecución con configuración personalizada
    python run_etl.py --config /ruta/a/mi_config.yaml
    
    # Modo dry-run (solo validaciones)
    python run_etl.py --dry-run
    
    # Ejecución con logging detallado
    python run_etl.py --verbose
    
    # Ejecución en modo silencioso
    python run_etl.py --quiet

Autor: Sinergia Digital
Fecha: Junio 2025
Versión: 1.0.3
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
import subprocess
import yaml
from typing import Optional, Dict, Any

# Importar el sistema ETL principal
try:
    from etl_main import ETLPipeline
except ImportError as e:
    print(f"❌ Error: No se pudo importar el módulo ETL principal: {e}")
    print("   Asegúrese de que etl_main.py esté en el directorio actual.")
    sys.exit(1)


class ETLRunner:
    """
    Orquestador para la ejecución del pipeline ETL
    Maneja argumentos, validaciones previas y coordinación general
    """
    
    def __init__(self):
        """Inicializa el runner del ETL"""
        self.args = None
        self.config = None
        self.start_time = datetime.now()
        
    def setup_argument_parser(self) -> argparse.ArgumentParser:
        """
        Configura el parser de argumentos de línea de comandos
        
        Returns:
            ArgumentParser configurado
        """
        parser = argparse.ArgumentParser(
            description='Sistema ETL para E-commerce Unificado',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Ejemplos de uso:
  %(prog)s                              # Ejecución normal
  %(prog)s --config custom_config.yaml  # Con configuración personalizada
  %(prog)s --dry-run                    # Solo validaciones, no procesa datos
  %(prog)s --verbose                    # Logging detallado
  %(prog)s --quiet                      # Solo errores críticos
  %(prog)s --validate-config            # Solo valida configuración

Para más información, consulte README.md
            """
        )
        
        # Argumentos principales
        parser.add_argument(
            '--config', '-c',
            type=str,
            default='config.yaml',
            help='Ruta al archivo de configuración (default: config.yaml)'
        )
        
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Ejecutar en modo dry-run (validaciones sin procesamiento real)'
        )
        
        parser.add_argument(
            '--verbose', '-v',
            action='store_true',
            help='Habilitar logging detallado (DEBUG level)'
        )
        
        parser.add_argument(
            '--quiet', '-q',
            action='store_true',
            help='Ejecutar en modo silencioso (solo errores críticos)'
        )
        
        parser.add_argument(
            '--validate-config',
            action='store_true',
            help='Solo validar configuración y salir'
        )
        
        parser.add_argument(
            '--log-file',
            type=str,
            help='Archivo de log personalizado (sobrescribe configuración)'
        )
        
        parser.add_argument(
            '--sample-size',
            type=int,
            help='Procesar solo una muestra de N registros (para pruebas)'
        )
        
        parser.add_argument(
            '--skip-validations',
            action='store_true',
            help='Saltar validaciones de calidad de datos (no recomendado en producción)'
        )
        
        parser.add_argument(
            '--version',
            action='version',
            version='ETL E-commerce Unificado v1.0.3'
        )
        
        return parser
    
    def validate_prerequisites(self) -> bool:
        """
        Valida que todos los pre-requisitos estén cumplidos
        
        Returns:
            True si todas las validaciones pasan
        """
        print("🔍 Validando pre-requisitos del sistema...")
        
        validation_errors = []
        
        # 1. Verificar archivo de configuración
        config_path = Path(self.args.config)
        if not config_path.exists():
            validation_errors.append(f"Archivo de configuración no encontrado: {config_path}")
        elif not config_path.suffix.lower() in ['.yaml', '.yml']:
            validation_errors.append(f"Archivo de configuración debe ser YAML: {config_path}")
        
        # 2. Intentar cargar configuración
        if not validation_errors:
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    self.config = yaml.safe_load(f)
                print(f"✅ Configuración cargada desde: {config_path}")
            except Exception as e:
                validation_errors.append(f"Error cargando configuración: {e}")
        
        # 3. Validar estructura de configuración
        if self.config and not validation_errors:
            required_sections = ['databases', 'data_sources', 'etl_settings']
            for section in required_sections:
                if section not in self.config:
                    validation_errors.append(f"Sección faltante en configuración: {section}")
        
        # 4. Validar directorios requeridos
        if self.config:
            # Directorio de logs
            log_dir = Path(self.config.get('logging', {}).get('log_dir', 'logs'))
            try:
                log_dir.mkdir(parents=True, exist_ok=True)
                print(f"✅ Directorio de logs: {log_dir}")
            except Exception as e:
                validation_errors.append(f"No se puede crear directorio de logs {log_dir}: {e}")
            
            # Archivo Excel
            excel_path = Path(self.config.get('data_sources', {}).get('excel', {}).get('file_path', ''))
            if not excel_path.exists():
                validation_errors.append(f"Archivo Excel no encontrado: {excel_path}")
            else:
                print(f"✅ Archivo Excel encontrado: {excel_path}")
        
        # 5. Verificar librerías Python requeridas
        required_packages = [
            'pandas', 'numpy', 'sqlalchemy', 'psycopg2', 'pyodbc', 'openpyxl', 'yaml'
        ]
        
        missing_packages = []
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                # Intentar nombres alternativos
                if package == 'psycopg2':
                    try:
                        __import__('psycopg2-binary')
                    except ImportError:
                        missing_packages.append(package)
                elif package == 'yaml':
                    try:
                        __import__('pyyaml')
                    except ImportError:
                        missing_packages.append(package)
                else:
                    missing_packages.append(package)
        
        if missing_packages:
            validation_errors.append(f"Paquetes Python faltantes: {', '.join(missing_packages)}")
        else:
            print("✅ Todas las librerías Python requeridas están disponibles")
        
        # 6. Mostrar errores si los hay
        if validation_errors:
            print("\n❌ Pre-requisitos no cumplidos:")
            for i, error in enumerate(validation_errors, 1):
                print(f"   {i}. {error}")
            return False
        
        print("✅ Todos los pre-requisitos cumplidos")
        return True
    
    def validate_config_only(self) -> bool:
        """
        Realiza validación completa de configuración y muestra resumen
        
        Returns:
            True si la configuración es válida
        """
        print("📋 Validando configuración detallada...")
        
        if not self.config:
            print("❌ No hay configuración para validar")
            return False
        
        # Mostrar resumen de configuración
        print("\n📊 Resumen de configuración:")
        print("-" * 50)
        
        # Bases de datos
        db_config = self.config.get('databases', {})
        if 'sql_server' in db_config:
            sql_config = db_config['sql_server']
            print(f"SQL Server: {sql_config.get('host')}:{sql_config.get('port')}/{sql_config.get('database')}")
        
        if 'postgresql' in db_config:
            pg_config = db_config['postgresql']
            print(f"PostgreSQL: {pg_config.get('host')}:{pg_config.get('port')}/{pg_config.get('database')}")
        
        # Fuentes de datos
        data_sources = self.config.get('data_sources', {})
        if 'excel' in data_sources:
            excel_config = data_sources['excel']
            print(f"Excel: {excel_config.get('file_path')}")
        
        # Configuración ETL
        etl_settings = self.config.get('etl_settings', {})
        print(f"Chunk size: {etl_settings.get('chunk_size', 'No configurado')}")
        print(f"Max retries: {etl_settings.get('max_retries', 'No configurado')}")
        
        # Reglas de negocio
        business_rules = self.config.get('business_rules', {})
        print(f"Días cliente activo: {business_rules.get('dias_cliente_activo', 'No configurado')}")
        
        # Logging
        logging_config = self.config.get('logging', {})
        print(f"Log level: {logging_config.get('level', 'No configurado')}")
        print(f"Log dir: {logging_config.get('log_dir', 'No configurado')}")
        
        print("-" * 50)
        print("✅ Configuración validada exitosamente")
        
        return True
    
    def setup_custom_logging(self) -> None:
        """Configura logging personalizado basado en argumentos"""
        if self.args.verbose:
            # Logging detallado
            logging.basicConfig(
                level=logging.DEBUG,
                format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            print("🔍 Logging detallado habilitado")
        
        elif self.args.quiet:
            # Solo errores críticos
            logging.basicConfig(
                level=logging.ERROR,
                format='%(asctime)s | %(levelname)s | %(message)s'
            )
            print("🔇 Modo silencioso habilitado")
    
    def apply_runtime_overrides(self) -> None:
        """Aplica modificaciones de configuración basadas en argumentos"""
        if not self.config:
            return
        
        # Override de log file
        if self.args.log_file:
            self.config.setdefault('logging', {})['filename'] = self.args.log_file
            print(f"📝 Log file personalizado: {self.args.log_file}")
        
        # Override de sample size
        if self.args.sample_size:
            self.config.setdefault('development', {})['sample_data'] = {
                'enabled': True,
                'max_records': self.args.sample_size
            }
            print(f"🔢 Procesando muestra de {self.args.sample_size:,} registros")
        
        # Skip validations
        if self.args.skip_validations:
            self.config.setdefault('development', {})['skip_validations'] = True
            print("⚠️  Validaciones de calidad deshabilitadas")
        
        # Dry run mode
        if self.args.dry_run:
            self.config.setdefault('development', {})['dry_run'] = True
            print("🔍 Modo dry-run habilitado")
    
    def run_etl_pipeline(self) -> bool:
        """
        Ejecuta el pipeline ETL principal
        
        Returns:
            True si la ejecución es exitosa
        """
        try:
            print(f"\n🚀 Iniciando pipeline ETL - {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            
            if self.args.dry_run:
                print("📋 MODO DRY-RUN: Solo se ejecutarán validaciones")
            
            # Crear instancia del pipeline
            pipeline = ETLPipeline()
            pipeline.config = self.config  # Usar configuración modificada
            
            # Ejecutar pipeline
            if self.args.dry_run:
                # En modo dry-run, solo ejecutar validaciones
                success = self._run_dry_run(pipeline)
            else:
                # Ejecución completa
                success = pipeline.run()
            
            return success
            
        except KeyboardInterrupt:
            print("\n⚠️  Ejecución interrumpida por el usuario")
            return False
        except Exception as e:
            print(f"\n💥 Error crítico durante la ejecución: {e}")
            logging.exception("Error crítico en pipeline ETL")
            return False
    
    def _run_dry_run(self, pipeline) -> bool:
        """
        Ejecuta validaciones en modo dry-run
        
        Args:
            pipeline: Instancia del pipeline ETL
            
        Returns:
            True si todas las validaciones pasan
        """
        print("🔍 Ejecutando validaciones en modo dry-run...")
        
        try:
            # Validar conexiones
            print("\n1️⃣ Validando conexión a SQL Server...")
            try:
                engine = pipeline.extractor.connect_sql_server()
                with engine.connect() as conn:
                    result = conn.execute("SELECT COUNT(*) as total FROM clientes").fetchone()
                    print(f"   ✅ Conexión exitosa - {result[0]:,} clientes encontrados")
            except Exception as e:
                print(f"   ❌ Error conectando SQL Server: {e}")
                return False
            
            print("\n2️⃣ Validando conexión a PostgreSQL...")
            try:
                engine = pipeline.loader.connect_postgresql()
                with engine.connect() as conn:
                    result = conn.execute("SELECT 1").fetchone()
                    print("   ✅ Conexión a PostgreSQL exitosa")
            except Exception as e:
                print(f"   ❌ Error conectando PostgreSQL: {e}")
                return False
            
            print("\n3️⃣ Validando archivo Excel...")
            try:
                excel_data = pipeline.extractor.extract_from_excel()
                print(f"   ✅ Excel leído exitosamente - {len(excel_data):,} registros")
            except Exception as e:
                print(f"   ❌ Error leyendo Excel: {e}")
                return False
            
            print("\n4️⃣ Validando estructura de tablas destino...")
            try:
                engine = pipeline.loader.connect_postgresql()
                with engine.connect() as conn:
                    # Verificar que las tablas existen
                    tables_result = conn.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name IN ('dim_clientes', 'fact_compras')
                    """).fetchall()
                    
                    found_tables = [row[0] for row in tables_result]
                    required_tables = ['dim_clientes', 'fact_compras']
                    
                    if set(required_tables).issubset(set(found_tables)):
                        print("   ✅ Tablas destino encontradas")
                    else:
                        missing = set(required_tables) - set(found_tables)
                        print(f"   ❌ Tablas faltantes: {missing}")
                        return False
            except Exception as e:
                print(f"   ❌ Error validando tablas: {e}")
                return False
            
            print("\n✅ Todas las validaciones de dry-run pasaron exitosamente")
            return True
            
        except Exception as e:
            print(f"\n❌ Error durante dry-run: {e}")
            return False
    
    def print_execution_summary(self, success: bool) -> None:
        """
        Muestra resumen de ejecución
        
        Args:
            success: Si la ejecución fue exitosa
        """
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        print("\n" + "=" * 60)
        if success:
            print("🎉 EJECUCIÓN COMPLETADA EXITOSAMENTE")
        else:
            print("❌ EJECUCIÓN FALLIDA")
        
        print("=" * 60)
        print(f"Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Fin:    {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duración: {duration}")
        
        if self.config:
            log_file = self.config.get('logging', {}).get('filename', 'etl_system.log')
            log_dir = self.config.get('logging', {}).get('log_dir', 'logs')
            print(f"Logs: {log_dir}/{log_file}")
        
        print("=" * 60)
    
    def run(self) -> int:
        """
        Método principal de ejecución
        
        Returns:
            Código de salida (0 = éxito, 1 = error)
        """
        # Configurar parser de argumentos
        parser = self.setup_argument_parser()
        self.args = parser.parse_args()
        
        # Configurar logging personalizado
        self.setup_custom_logging()
        
        # Validar pre-requisitos
        if not self.validate_prerequisites():
            return 1
        
        # Aplicar overrides de configuración
        self.apply_runtime_overrides()
        
        # Validación solo de configuración
        if self.args.validate_config:
            if self.validate_config_only():
                print("\n✅ Validación de configuración exitosa")
                return 0
            else:
                print("\n❌ Validación de configuración falló")
                return 1
        
        # Ejecutar ETL
        success = self.run_etl_pipeline()
        
        # Mostrar resumen
        self.print_execution_summary(success)
        
        return 0 if success else 1


def main():
    """Función principal del script"""
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     ETL E-COMMERCE UNIFICADO v1.0.3                          ║
║                                                                              ║
║  Sistema de procesamiento ETL para unificación de datos online/offline       ║
║  Desarrollado por: Sinergia Digital                                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """)
    
    runner = ETLRunner()
    exit_code = runner.run()
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
