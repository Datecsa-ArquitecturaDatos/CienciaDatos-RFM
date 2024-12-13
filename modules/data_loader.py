
"""
Proyecto: Demo RFM
Módulo: data_loader.py
Versión: 1.0
Fecha de creación: 2024-12-10
Autor: Carolina Torres Zapata
Modificado por: 
Fecha modificación: 
Descripción:
    La clase DataLoader permite cargar datos desde múltiples fuentes (CSV, Excel, Parquet, etc),
    utilizando configuraciones definidas en un archivo YAML. También gestiona el cálculo del rango
    de fechas para análisis RFM y filtra los datos según este rango, si es necesario.

    Métodos principales:
    - __init__: Constructor que inicializa la configuración y calcula el rango de fechas RFM.
    - load_config: Carga la configuración desde un archivo YAML.
    - get_date_range_for_rfm: Calcula el rango de fechas a partir de las configuraciones.
    - load_from_csv: Carga datos desde un archivo CSV.
    - load_from_excel: Carga datos desde un archivo Excel.
    - load_from_parquet: Carga datos desde un archivo Parquet.
    - _process_dates_and_filter: Procesa columnas de fechas y aplica filtros por rango de fechas.

"""

## Importe de Librerías
import os
import pandas as pd
import yaml


class DataLoader:
    def __init__(self, config_path: str):
        """
        Constructor de la clase DataLoader.

        Parámetros:
            - config_path (str): Ruta al archivo YAML que contiene la configuración.

        Atributos inicializados:
            - self.config (dict): Diccionario con las configuraciones cargadas desde el YAML.
            - self.start_date (pd.Timestamp): Fecha de inicio para el análisis RFM.
            - self.end_date (pd.Timestamp): Fecha de fin para el análisis RFM.
        """
        self.config = self.load_config(config_path)
        self.start_date, self.end_date = self.get_date_range_for_rfm()

    @staticmethod
    def load_config(config_path: str) -> dict:
        """
        Carga el archivo de configuración en formato YAML.

        Parámetros:
            - config_path (str): Ruta al archivo YAML.

        Retorna:
            - dict: Configuración cargada como un diccionario de Python.

        Excepciones:
            - FileNotFoundError: Si el archivo YAML no existe.
            - ValueError: Si ocurre un error al leer el archivo YAML.
        """
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"No se encontró el archivo de configuración: {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error al leer el archivo YAML: {e}")

    
    ## Calcular Rango de Fecha
    def get_date_range_for_rfm(self) -> (pd.Timestamp, pd.Timestamp):
        """
        Calcula el rango de fechas para el análisis RFM con base en la configuración.

        Retorna:
            - (pd.Timestamp, pd.Timestamp): Tupla con la fecha de inicio y fin del rango.

        Excepciones:
            - ValueError: Si el intervalo definido en la configuración no es reconocido.
        """
        current_date = pd.Timestamp.now()
        interval = self.config['global_settings']['date_range']['interval']
        number = self.config['global_settings']['date_range']['number']
        end_date = pd.Timestamp(current_date.year, current_date.month, 1) - pd.Timedelta(days=1)
        if interval == 'months':
            start_date = end_date - pd.DateOffset(months=number)
        elif interval == 'days':
            start_date = end_date - pd.Timedelta(days=number)
        elif interval == 'years':
            start_date = end_date - pd.DateOffset(years=number)
        else:
            raise ValueError(f"Intervalo '{interval}' no reconocido. Usa 'months', 'days' o 'years'.")
        
        # Sólo para efectos de esta prueba quitar después y ajustar código
        start_date = pd.to_datetime("2023-12-01")
        end_date = pd.to_datetime("2024-12-10")

        return start_date, end_date

    
    ## Cargar CSV
    def load_from_csv(self, csv_key: str, chunksize: int = None, filter_dates: bool = True) -> pd.DataFrame:
        """
        Carga datos desde un archivo CSV según las configuraciones.

        Parámetros:
            - csv_key (str): Clave del archivo CSV en la configuración YAML.
            - chunksize (int, opcional): Tamaño de los fragmentos de datos a cargar (modo por partes).
            - filter_dates (bool, opcional): Si se aplica el filtro por rango de fechas.

        Retorna:
            - pd.DataFrame: Datos cargados.

        Excepciones:
            - ValueError: Si la clave especificada no existe en la configuración.
            - FileNotFoundError: Si el archivo CSV no se encuentra.
        """
        csv_config = self.config['data_sources']['csv_sources'].get(csv_key)
        if not csv_config:
            raise ValueError(f"No se encontró la configuración para '{csv_key}' en el archivo YAML.")
        file_path = csv_config.get('path')
        delimiter = csv_config.get('delimiter', ',')
        parse_dates = csv_config.get('parse_dates', [])
        selected_columns = csv_config.get('select_columns', None)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo CSV no existe en la ruta: {file_path}")

        data = pd.DataFrame()
        if chunksize:
            for chunk in pd.read_csv(file_path, delimiter=delimiter, parse_dates=parse_dates, chunksize=chunksize, usecols=selected_columns):
                chunk = self._process_dates_and_filter(chunk, parse_dates, filter_dates)
                data = pd.concat([data, chunk], ignore_index=True)
        else:
            data = pd.read_csv(file_path, delimiter=delimiter, parse_dates=parse_dates, usecols=selected_columns)
            data = self._process_dates_and_filter(data, parse_dates, filter_dates)

        return data

    
    ## Cargar Excel
    def load_from_excel(self, excel_key: str, filter_dates: bool = True) -> pd.DataFrame:
        """
        Carga datos desde un archivo Excel según las configuraciones.

        Parámetros:
            - excel_key (str): Clave del archivo Excel en la configuración YAML.
            - filter_dates (bool, opcional): Si se aplica el filtro por rango de fechas.

        Retorna:
            - pd.DataFrame: Datos cargados.

        Excepciones:
            - ValueError: Si la clave especificada no existe en la configuración.
            - FileNotFoundError: Si el archivo Excel no se encuentra.
        """
        excel_config = self.config['data_sources']['excel_sources'].get(excel_key)
        if not excel_config:
            raise ValueError(f"No se encontró la configuración para '{excel_key}' en el archivo YAML.")
        file_path = excel_config.get('path')
        sheet_name = excel_config.get('sheet_name', 0)
        parse_dates = excel_config.get('parse_dates', [])
        selected_columns = excel_config.get('select_columns', None)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo Excel no existe en la ruta: {file_path}")

        data = pd.read_excel(file_path, sheet_name=sheet_name, usecols=selected_columns, parse_dates=parse_dates)
        data = self._process_dates_and_filter(data, parse_dates, filter_dates)

        return data

    
    ## Cargar Parquet
    def load_from_parquet(self, parquet_key: str, filter_dates: bool = True) -> pd.DataFrame:
        """
        Carga datos desde un archivo Parquet según las configuraciones.

        Parámetros:
            - parquet_key (str): Clave del archivo Parquet en la configuración YAML.
            - filter_dates (bool, opcional): Si se aplica el filtro por rango de fechas.

        Retorna:
            - pd.DataFrame: Datos cargados.

        Excepciones:
            - ValueError: Si la clave especificada no existe en la configuración.
            - FileNotFoundError: Si el archivo Parquet no se encuentra.
        """
        parquet_config = self.config['data_sources']['parquet_sources'].get(parquet_key)
        if not parquet_config:
            raise ValueError(f"No se encontró la configuración para '{parquet_key}' en el archivo YAML.")
        file_path = parquet_config.get('path')
        selected_columns = parquet_config.get('select_columns', None)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo Parquet no existe en la ruta: {file_path}")

        data = pd.read_parquet(file_path, columns=selected_columns)
        parse_dates = parquet_config.get('parse_dates', [])
        data = self._process_dates_and_filter(data, parse_dates, filter_dates)

        return data

    
    ## Filtrar Rango de Fechas
    def _process_dates_and_filter(self, data: pd.DataFrame, parse_dates: list, filter_dates: bool) -> pd.DataFrame:
        """
        Procesa las columnas de fechas y aplica el filtro por rango si es necesario.

        Parámetros:
            - data (pd.DataFrame): Datos a procesar.
            - parse_dates (list): Lista de columnas a convertir a datetime.
            - filter_dates (bool): Si se debe aplicar el filtro de rango de fechas.

        Retorna:
            - pd.DataFrame: Datos procesados.
        """
        for date_col in parse_dates:
            data[date_col] = pd.to_datetime(data[date_col], errors='coerce')
        if filter_dates and parse_dates:
            data = data[(data[parse_dates[0]] >= self.start_date) & (data[parse_dates[0]] <= self.end_date)]
        return data
