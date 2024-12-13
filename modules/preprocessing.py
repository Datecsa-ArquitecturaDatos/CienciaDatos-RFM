"""
Proyecto: Demo RFM
Módulo: preprocessing.py
Versión: 1.0
Fecha de creación: 2024-12-10
Autor: Carolina Torres Zapata
Modificado por: 
Fecha modificación: 
Descripción:
    Este módulo está diseñado para realizar el preprocesamiento de datos de forma flexible y modular. 
    Utiliza configuraciones definidas en un archivo YAML, lo que permite adaptar el flujo de trabajo según los 
    requerimientos específicos de cada fuente de datos.
"""

import pandas as pd
import yaml
from modules.data_loader import DataLoader

# Funciones de preprocesamiento

def handle_missing_values(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    Maneja valores faltantes en las columnas especificadas del DataFrame.

    Parámetros:
        - df (pd.DataFrame): DataFrame a procesar.
        - params (dict): Diccionario con la estrategia para manejar valores faltantes.
            - "strategy" (dict): Mapeo columna-acción, donde las acciones soportadas son:
                - "drop": Elimina filas con valores nulos en la columna especificada.
                - "mean": Reemplaza valores nulos con la media de la columna.
                - "median": Reemplaza valores nulos con la mediana de la columna.
                - "zero": Reemplaza valores nulos con el valor 0.

    Retorna:
        - pd.DataFrame: DataFrame procesado con los valores faltantes manejados según la estrategia definida.

    """
    strategy = params.get("strategy", {})
    for column, action in strategy.items():
        if column not in df.columns:
            continue

        if action == 'drop':
            df = df.dropna(subset=[column])
        elif action == 'mean':
            df[column].fillna(df[column].mean(), inplace=True)
        elif action == 'median':
            df[column].fillna(df[column].median(), inplace=True)
        elif action == 'zero':
            df[column].fillna(0, inplace=True)
    return df

def remove_negative_values(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    Elimina filas del DataFrame donde las columnas especificadas contienen valores negativos.

    Parámetros:
        - df (pd.DataFrame): DataFrame a procesar.
        - params (dict): Diccionario con la configuración para eliminar valores negativos.
            - "columns" (list): Lista de nombres de columnas en las que se evaluará la existencia de valores negativos.

    Retorna:
        - pd.DataFrame: DataFrame sin filas que contengan valores negativos en las columnas indicadas.
    """
    columns = params.get("columns", [])
    for column in columns:
        if column in df.columns:
            df = df[df[column] >= 0]
    return df

def handle_duplicates(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    Elimina filas duplicadas en el DataFrame según las configuraciones especificadas.

    Parámetros:
        - df (pd.DataFrame): DataFrame a procesar.
        - params (dict): Diccionario con las configuraciones para el manejo de duplicados.
            - "subset" (list o None): Lista de columnas a considerar para identificar duplicados. 
              Si es None, se usan todas las columnas del DataFrame.
            - "keep" (str): Especifica qué duplicado conservar. Opciones:
                - 'first' (por defecto): Conserva la primera aparición.
                - 'last': Conserva la última aparición.
                - False: Elimina todas las filas duplicadas.

    Retorna:
        - pd.DataFrame: DataFrame sin duplicados según las configuraciones.

    """
    subset = params.get("subset", None)
    keep = params.get("keep", 'first')
    if subset is None:
        subset = df.columns
    return df.drop_duplicates(subset=subset, keep=keep)

def cast_column_types(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    Convierte los tipos de datos de las columnas especificadas en un DataFrame según el mapeo definido en el archivo de configuración YAML.

    Parámetros:
        - df (pd.DataFrame): DataFrame a procesar.
        - params (dict): Diccionario con las configuraciones de conversión.
            - "cast_map" (dict): Diccionario que mapea nombres de columnas a los tipos de datos deseados.
              Los tipos pueden incluir:
                - Tipos básicos de pandas como "int", "float", "str".
                - Tipo especial "datetime" para convertir a formato de fecha y hora.

    Retorna:
        - pd.DataFrame: DataFrame con las columnas convertidas a los tipos especificados.

    """
    cast_map = params.get("cast_map", {})
    for column, dtype in cast_map.items():
        if column in df.columns:
            try:
                if dtype == "datetime":
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                else:
                    df[column] = df[column].astype(dtype)
            except Exception as e:
                pass  # Mejor registrar errores en lugar de imprimir
    return df

# Diccionario de funciones disponibles
AVAILABLE_STEPS = {
    "handle_missing_values": handle_missing_values,
    "remove_negative_values": remove_negative_values,
    "handle_duplicates": handle_duplicates,
    "cast_column_types": cast_column_types,
}

class DataPreprocessor:
    def __init__(self, config_path: str):
        """
        Inicializa la clase con la configuración del archivo YAML que define los pasos de preprocesamiento.
        
        Este constructor carga el archivo de configuración YAML y almacena los pasos de preprocesamiento 
        para ser aplicados posteriormente a los DataFrames proporcionados.

        Parámetros:
            - config_path: str
                Ruta del archivo YAML que contiene los pasos de preprocesamiento a aplicar a los datos.
                El archivo debe tener una estructura que incluya una lista de pasos bajo la clave 'preprocessing_steps'.
                Cada paso debe contener el nombre de la operación ('step') y los parámetros necesarios ('params').

        Atributos:
            - self.config: dict
                Contiene la configuración cargada desde el archivo YAML.
            - self.steps_config: dict
                Un diccionario que mapea cada fuente de datos a sus respectivos pasos de preprocesamiento.
        """
        self.config = self.load_config(config_path)
        self.steps_config = self.config.get("preprocessing_steps", {})
        self.data_loader = DataLoader(config_path=config_path)

    def load_config(self, config_path: str) -> dict:
        """ Carga el archivo de configuración YAML. """
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except (FileNotFoundError, yaml.YAMLError) as e:
            raise ValueError(f"Error al cargar el archivo YAML: {e}")

    def apply_preprocessing_to_source(self, df: pd.DataFrame, source_key: str) -> pd.DataFrame:
        """
        Aplica los pasos de preprocesamiento definidos en el archivo YAML a un DataFrame específico.
        
        Parámetros:
            - df: pd.DataFrame
                DataFrame sobre el que se aplicarán los pasos de preprocesamiento.
            - source_key: str
                Identificador de la fuente, utilizado para buscar los pasos correspondientes.
        
        Retorna:
            - pd.DataFrame
                DataFrame procesado.
        """
        steps = self.steps_config.get(source_key, [])
        if not steps:
            return df  # Devuelve el DataFrame sin modificar si no hay pasos configurados

        for step_config in steps:
            step_name = step_config.get("step")
            params = step_config.get("params", {})

            # Validación de existencia del paso
            if step_name not in AVAILABLE_STEPS:
                continue

            # Aplicar el paso de preprocesamiento
            df = AVAILABLE_STEPS[step_name](df, params)

        return df

    def apply_preprocessing_to_all_sources(self, dataframes: dict) -> dict:
        """
        Aplica los pasos de preprocesamiento a múltiples fuentes de datos.
        
        Parámetros:
            - dataframes: dict
                Diccionario de DataFrames, con claves como identificadores de fuentes.
        
        Retorna:
            - dict
                Diccionario de DataFrames procesados.
        """
        return {
            source_key: self.apply_preprocessing_to_source(df, source_key)
            for source_key, df in dataframes.items()
        }