
"""
Proyecto: Demo RFM
Módulo: exporter.py
Versión: 1.0
Fecha de creación: 2024-12-10
Autor: Carolina Torres Zapata
Modificado por: 
Fecha modificación: 
Descripción:
 Este módulo contiene la clase `DataExporter` que proporciona métodos para exportar 
    datos en diferentes formatos (CSV, Excel, Parquet, SQL) de acuerdo con las configuraciones 
    definidas en el archivo YAML. La configuración es cargada a través del módulo `DataLoader`.
    Los métodos de exportación permiten guardar los datos en los formatos correspondientes según 
    las claves de configuración especificadas en el YAML.

Funciones principales:
    - export_to_csv: Exporta un DataFrame a un archivo CSV.
    - export_to_excel: Exporta un DataFrame a un archivo Excel.
    - export_to_parquet: Exporta un DataFrame a un archivo Parquet.
    - export_to_sql: Exporta un DataFrame a una base de datos SQL.
"""

### Importar Librerías
import os
import pandas as pd
import yaml
from modules.data_loader import DataLoader

class DataExporter:
    def __init__(self, config_path: str):
       
        self.config = DataLoader.load_config(config_path)

    def export_to_csv(self, data: pd.DataFrame, csv_key: str) -> None:
        """
        Exporta los datos a un archivo CSV según la configuración especificada en el YAML.
        
        :param data: DataFrame con los datos a exportar.
        :param csv_key: Clave en el archivo YAML que contiene las opciones de exportación CSV.
        """
        try:
            csv_config = self.config['export_settings']['csv_sources'].get(csv_key)
            if not csv_config:
                raise ValueError(f"No se encontró la configuración para '{csv_key}' en el archivo YAML.")
            
            output_path = csv_config.get('path')
            data.to_csv(output_path, index=False)
            print(f"Datos exportados a CSV en {output_path}")
        except Exception as e:
            print(f"Error al exportar a CSV: {e}")

    def export_to_excel(self, data: pd.DataFrame, excel_key: str) -> None:
        """
        Exporta los datos a un archivo Excel según la configuración especificada en el YAML.
        
        :param data: DataFrame con los datos a exportar.
        :param excel_key: Clave en el archivo YAML que contiene las opciones de exportación Excel.
        """
        try:
            excel_config = self.config['export_settings']['excel_sources'].get(excel_key)
            if not excel_config:
                raise ValueError(f"No se encontró la configuración para '{excel_key}' en el archivo YAML.")
            
            output_path = excel_config.get('path')
            data.to_excel(output_path, index=False)
            print(f"Datos exportados a Excel en {output_path}")
        except Exception as e:
            print(f"Error al exportar a Excel: {e}")

    def export_to_parquet(self, data: pd.DataFrame, parquet_key: str) -> None:
        """
        Exporta los datos a un archivo Parquet según la configuración especificada en el YAML.
        
        :param data: DataFrame con los datos a exportar.
        :param parquet_key: Clave en el archivo YAML que contiene las opciones de exportación Parquet.
        """
        try:
            parquet_config = self.config['export_settings']['parquet_sources'].get(parquet_key)
            if not parquet_config:
                raise ValueError(f"No se encontró la configuración para '{parquet_key}' en el archivo YAML.")
            
            output_path = parquet_config.get('path')
            data.to_parquet(output_path, index=False)
            print(f"Datos exportados a Parquet en {output_path}")
        except Exception as e:
            print(f"Error al exportar a Parquet: {e}")

    def export_to_sql(self, data: pd.DataFrame, sql_key: str) -> None:
        """
        Exporta los datos a una base de datos SQL según la configuración especificada en el YAML.
        
        :param data: DataFrame con los datos a exportar.
        :param sql_key: Clave en el archivo YAML que contiene las opciones de exportación SQL.
        """
        try:
            sql_config = self.config['export_settings']['sql_sources'].get(sql_key)
            if not sql_config:
                raise ValueError(f"No se encontró la configuración para '{sql_key}' en el archivo YAML.")
            
            # Obtener configuración de la conexión a la base de datos
            db_url = sql_config.get('db_url')
            table_name = sql_config.get('table_name')
            engine = create_engine(db_url)
            data.to_sql(table_name, con=engine, index=False, if_exists='replace')
            print(f"Datos exportados a SQL en la tabla {table_name}")
        except Exception as e:
            print(f"Error al exportar a SQL: {e}")
