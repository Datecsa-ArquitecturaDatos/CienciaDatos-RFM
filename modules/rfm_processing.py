
"""
Proyecto: Demo RFM
Módulo: rfm_processing.py
Versión: 1.0
Fecha de creación: 2024-12-10
Autor: Carolina Torres Zapata
Modificado por: 
Fecha modificación: 
Descripción:
    Este módulo contiene la clase RFMProcessing, encargada del procesamiento de los datos RFM (Recency, Frequency, Monetary).
    Los métodos de esta clase permiten realizar estos cálculos de acuerdo con la configuración definida en el archivo YAML proporcionado.
    
    Los procesos que realiza este módulo incluyen:
    - Cálculo de límites de outliers utilizando diferentes métodos (IQR, desviación estándar, percentiles).
    - Cálculo de puntos de corte mediante percentiles o el método Jenks.
    - Asignación de puntajes a los datos en función de los puntos de corte, con soporte para puntuaciones inversas.
    - Determinación de los rangos en los que caen los valores según los puntos de corte.
    
    Este módulo es parte de un sistema de análisis de datos RFM, y es utilizado para transformar los datos en un formato adecuado 
    para segmentar a los clientes según su comportamiento y su relación con la empresa.

    
"""

### Importar Librerías
import numpy as np
import pandas as pd
import jenkspy
from modules.data_loader import DataLoader

class RFMProcessing:
    
    def __init__(self, config_path: str):
        """
        Inicializa el módulo de procesamiento de RFM con la configuración especificada.
        """
        # Cargar configuración desde el archivo YAML
        self.config = DataLoader.load_config(config_path)
        self.global_config = self.config["global_settings"]
        self.variables_config = self.config["variables"]

    ## Manejo de Outliers
    def calculate_outliers_limits(self, df: pd.DataFrame, column: str) -> tuple:
        """
        Calcula los límites de outliers (LI y LS) para una columna basada en la configuración.

        Parámetros:
            - df (pd.DataFrame): El DataFrame que contiene los datos.
            - column (str): El nombre de la columna en la que se calcularán los límites de los outliers.

        Retorna:
            - tuple: Una tupla con los límites de outliers (LI y LS), el valor del método usado (IQR, std_dev o percentiles), 
                    y los valores mínimo y máximo de la columna.
                - LI (float): Límite inferior para los outliers.
                - LS (float): Límite superior para los outliers.
                - Un valor adicional dependiendo del método:
                    - Si el método es **IQR**: El valor de **IQR** calculado.
                    - Si el método es **std_dev**: El valor de la **desviación estándar**.
                    - Si el método es **percentiles**: `None`.
                - min_value (float): El valor mínimo de la columna.
                - max_value (float): El valor máximo de la columna.

        Excepciones:
            - KeyError: Si la columna no está configurada en el archivo YAML.
            - ValueError: Si el método de manejo de outliers no está soportado.

        """
        var_config = self.variables_config.get(column)
        if var_config is None:
            raise KeyError(f"La columna '{column}' no está configurada en el YAML.")

        min_value = df[column].min()
        max_value = df[column].max()
        method = var_config["outlier_method"]

        if method == "IQR":
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR_value = Q3 - Q1
            # Factor IQR configurable desde el YAML
            IQR_factor = var_config.get("iqr_factor", 1.5)
            LI = Q1 - IQR_factor * IQR_value
            LS = Q3 + IQR_factor * IQR_value
            return LI, LS, IQR_value, min_value, max_value

        elif method == "std_dev":
            mean_value = df[column].mean()
            std_dev = df[column].std()
            # Factor std_dev configurable desde el YAML
            std_dev_factor = var_config.get("std_dev_factor", 2)
            LI = mean_value - std_dev_factor * std_dev
            LS = mean_value + std_dev_factor * std_dev
            return LI, LS, std_dev, min_value, max_value

        elif method == "percentiles":
            # Percentiles configurables desde el YAML
            lower_percentile = var_config.get("percentile_lower", 5)
            upper_percentile = var_config.get("percentile_upper", 95)
            LI = np.percentile(df[column], lower_percentile)
            LS = np.percentile(df[column], upper_percentile)
            return LI, LS, None, min_value, max_value

        else:
            raise ValueError(f"Método de manejo de outliers no soportado: {method}")

    
    ## Definición de Puntos de Corte
    def calculate_breaks(self, df: pd.DataFrame, column: str) -> np.ndarray:
        """
        Calcula los puntos de corte (breaks) para una columna según la configuración.

        Parámetros:
            - df (pd.DataFrame): El DataFrame que contiene los datos.
            - column (str): El nombre de la columna para la que se calcularán los puntos de corte.

        Retorna:
            - np.ndarray: Un arreglo de los puntos de corte calculados, incluyendo los límites inferior y superior.
            - list: Una lista de tuplas con los rangos de cada punto de corte.

        Excepciones:
            - ValueError: Si el método de cálculo de breaks no es soportado.


        Descripción del Proceso:
            La función calcula los puntos de corte (breaks) para la columna especificada en el DataFrame,
            basándose en los métodos definidos en la configuración (por ejemplo, percentiles o Jenks).
            - **Percentiles**: Los puntos de corte se calculan usando percentiles definidos en la configuración.
            - **Jenks**: Se utiliza el algoritmo Jenks para calcular los puntos de corte y dividir los datos en grupos óptimos.

            En ambos casos, se filtran los valores dentro de los límites (calculados previamente por los outliers) y se asegura que no haya solapamientos entre los rangos.
        """
        var_config = self.variables_config[column]
        LI, LS, _, min_value, max_value = self.calculate_outliers_limits(df, column)
        
        # Filtrar valores dentro de los límites y que no sean nulos
        df_filtered = df[(df[column] >= LI) & (df[column] <= LS) & df[column].notnull()]
        method = var_config["breaks_method"]

        if method == "percentiles":
            # Generación de puntos de corte por percentiles, ajustable por configuración
            percentiles = np.linspace(0, 100, self.global_config["num_categories"] + 1)[1:-1]
            breaks = np.percentile(df_filtered[column], percentiles)
            # Añadir los puntos de corte al inicio y fin
            breaks = np.concatenate(([min_value - 0.001], breaks, [max_value + 0.001]))
            # Calcular los rangos de cada break
            break_ranges = [(breaks[i], breaks[i+1]) for i in range(len(breaks)-1)]
            
            # Asegurarse de que no haya solapamientos y que el límite superior de un rango no sea el límite inferior del siguiente
            for i in range(len(break_ranges)-1):
                if break_ranges[i][1] >= break_ranges[i+1][0]:
                    break_ranges[i] = (break_ranges[i][0], break_ranges[i][1] - 0.001)
            
            return breaks, break_ranges

        elif method == "jenks":
            # Usar el método Jenks para obtener los puntos de corte
            breaks = jenkspy.jenks_breaks(df_filtered[column].values, n_classes=self.global_config["num_categories"])[1:-1]
            breaks = np.concatenate(([min_value - 0.001], breaks, [max_value + 0.001]))
            # Calcular los rangos de cada break
            break_ranges = [(breaks[i], breaks[i+1]) for i in range(len(breaks)-1)]

            # Asegurarse de que no haya solapamientos y que el límite superior de un rango no sea el límite inferior del siguiente
            for i in range(len(break_ranges)-1):
                if break_ranges[i][1] >= break_ranges[i+1][0]:
                    break_ranges[i] = (break_ranges[i][0], break_ranges[i][1] - 0.001)

            return breaks, break_ranges

        else:
            raise ValueError(f"Método de cálculo de breaks no soportado: {method}")

    
    def calculate_score(self, df: pd.DataFrame, column: str, breaks: np.ndarray, break_ranges: list, inverse: bool = False) -> pd.Series:
        """
        Calcula el puntaje (score) de los valores en una columna del DataFrame en función de puntos de corte (breaks) 
        y devuelve también los rangos correspondientes.

        Esta función categoriza los valores en `column` en base a un conjunto de puntos de corte (breaks) 
        y asigna puntajes según un rango definido en la configuración global. 
        También identifica los rangos de los puntos de corte en los que cae cada valor.

        Parámetros:
            df (pd.DataFrame): DataFrame que contiene la columna a evaluar.
            column (str): Nombre de la columna del DataFrame a categorizar.
            breaks (np.ndarray): Array de valores que definen los puntos de corte.
            break_ranges (list): Lista de rangos asociados a los breaks.
            inverse (bool): Si es `True`, el puntaje será inverso (puntajes más altos para valores más bajos). 
                            Por defecto, es `False`.

        Retorna:
            pd.Series, list: 
                - Serie con los puntajes asignados a cada valor en la columna.
                - Lista con los rangos correspondientes a cada valor.

        """
        score_min = self.global_config["score_range"]["min"]
        score_max = self.global_config["score_range"]["max"]
        score_step = self.global_config["score_range"]["step"]
        num_categories = self.global_config["num_categories"]

        # Asignar categorías usando np.digitize
        scores = np.digitize(df[column], breaks, right=False)  # Usar right=False para asignar el valor al intervalo inferior

        # Limitar los puntajes dentro del rango válido
        scores = np.clip(scores, score_min, num_categories)

        # Obtener los rangos correspondientes a cada valor de break
        value_ranges = [self.get_range_for_value(value, break_ranges) for value in df[column]]

        if inverse:
            scores = score_max - ((scores - score_min) * score_step)  # Puntaje inverso
        else:
            scores = score_min + ((scores - score_min) * score_step)  # Puntaje estándar

        return scores, value_ranges  # Devuelve el puntaje y los rangos de cada valor

    
    def get_range_for_value(self, value: float, break_ranges: list) -> tuple:
        """
            Devuelve el rango de break (intervalo) en el que cae un valor específico.

            Esta función evalúa un valor y determina a cuál intervalo definido en `break_ranges` pertenece.
            Está diseñada para casos en los que los valores deben clasificarse en rangos asociados 
            a puntos de corte (breaks).

            Parámetros:
                value (float): El valor que se desea clasificar.
                break_ranges (list): Lista de tuplas que representan los intervalos de los puntos de corte.
                                    Cada tupla es de la forma `(lower, upper)`.

            Returna:
                tuple:
                    - Una tupla `(lower, upper)` que indica el intervalo donde se encuentra el valor.
                None:
                    - Si el valor no pertenece a ningún intervalo, devuelve `None`.

        """
        for i, (lower, upper) in enumerate(break_ranges):
            if lower <= value < upper:  # Ajuste para asegurar que el valor cae dentro del rango correcto
                return (lower, upper)
        
        # Si el valor está exactamente en el límite superior del último intervalo
        if value == break_ranges[-1][1]:
            return break_ranges[-1]
        
        return None  # En caso de no encontrar un rango.


    def process_rfm_data(rfm_processor, rfm_data):
        """
        Procesa los datos de RFM (Recency, Frequency, Monetary) calculando puntajes y rangos
        para cada una de las variables (Recency, Frequency, Monetary) según la configuración definida
        en el archivo de configuración YAML.

        Esta función utiliza las configuraciones de corte y puntaje para calcular los puntajes 
        y rangos de cada columna (recency, frequency, monetary) y los agrega al DataFrame original 
        de RFM.

        Parámetros:
        rfm_processor (object): Instancia de la clase RFMProcessor, que contiene las configuraciones 
                                y los métodos necesarios para calcular los puntajes y rangos.
        rfm_data (DataFrame): DataFrame de pandas que contiene los datos de RFM (Recency, Frequency, Monetary) 
                            que deben ser procesados.

        Retorna:
        DataFrame: DataFrame con los datos originales de RFM más las columnas adicionales de puntajes 
                (score) y rangos (range) para cada variable (Recency, Frequency, Monetary).
        """
        scores_dict = {}
        for column, var_config in rfm_processor.variables_config.items():
            try:
                # Configuración de inverso por defecto según el tipo de variable
                inverse = True if column.lower() == "recency" else False

                # Obtener los puntos de corte y los rangos usando el método calculate_breaks
                breaks, break_ranges = rfm_processor.calculate_breaks(rfm_data, column)

                # Calcular el puntaje y los rangos para la columna
                scores, value_ranges = rfm_processor.calculate_score(rfm_data, column, breaks, break_ranges, inverse=inverse)

                # Agregar los resultados al diccionario
                scores_dict[column + "_score"] = scores
                scores_dict[column + "_range"] = value_ranges

            except KeyError:
                print(f"Configuración no encontrada para la variable: {column}")
            except Exception as e:
                print(f"Error procesando la variable '{column}': {e}")

        # Convertir el diccionario de resultados en un DataFrame
        scores_df = pd.DataFrame(scores_dict)
        # Concatenar el DataFrame original con los puntajes y rangos calculados
        df_resultado = pd.concat([rfm_data, scores_df], axis=1)
        return df_resultado

