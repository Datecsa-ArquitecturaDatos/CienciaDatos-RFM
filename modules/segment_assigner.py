"""
Proyecto: Demo RFM
Módulo: segment_assigner.py
Versión: 1.0
Fecha de creación: 2024-12-10
Autor: Carolina Torres Zapata
Modificado por: 
Fecha modificación: 
Descripción:
    Este módulo contiene la clase RFMProcessor, que permite calcular el puntaje RFM (Recencia, Frecuencia y Monto) y 
    asignar categorías de negocio personalizadas a los clientes, según las configuraciones definidas en el archivo YAML. 
    La clase incluye métodos flexibles para calcular el puntaje final utilizando diferentes enfoques 
    (combinación de variables, suma o promedio). Además, asigna categorías de negocio basadas en reglas personalizadas,
    utilizando los valores de RFM. El módulo también gestiona la identificación de clientes nuevos.
   
"""

### Importar Librerías
import pandas as pd
import yaml
from modules.data_loader import DataLoader


class RFMProcessor:
    
    def __init__(self, config_path: str):
        """
        Inicializa la clase con la configuración del archivo YAML para el cálculo RFM.
        
        Parámetros:
            - config_path: str
                Ruta del archivo YAML que contiene las configuraciones para el cálculo de RFM.
        """
        try:
            self.config = DataLoader.load_config(config_path)
            self.score_method = self.config.get("score_method", "combinación")
            self.business_categories = self.config.get("business_categories", {})

            # Rango de fechas para el análisis
            self.data_loader = DataLoader(config_path=config_path)
            self.start_date, self.end_date = self.data_loader.get_date_range_for_rfm()
        except (FileNotFoundError, ValueError) as e:
            print(f"Error al cargar el archivo de configuración: {e}")
            self.score_method = "combinación"
            self.business_categories = {}

    
    def calculate_final_score(self, df: pd.DataFrame) -> pd.Series:
        """
        Calcula el score final de los clientes basado en el método especificado en la configuración.
        
        Parámetros:
            - df: pd.DataFrame
                DataFrame con columnas de puntajes RFM ('Recency_Score', 'Frequency_Score', 'Monetary_Score').
        
        Retorna:
            - pd.Series
                Serie con el score final calculado para cada cliente.
        """
        if self.score_method == 'combinacion':
            return df['Recency_score'].astype(str) + df['Frequency_score'].astype(str) + df['Monetary_score'].astype(str)
        elif self.score_method == 'suma':
            return df[['Recency_score', 'Frequency_score', 'Monetary_score']].sum(axis=1)
        elif self.score_method == 'promedio':
            return df[['Recency_score', 'Frequency_score', 'Monetary_score']].mean(axis=1)
        else:
            raise ValueError(f"Método de cálculo de score '{self.score_method}' no reconocido.")

    def assign_business_categories(self, df: pd.DataFrame, score_column: str = "Final_Score") -> pd.DataFrame:
        """
        Asigna categorías de negocio a los clientes basado en el score final.
        
        Parámetros:
            - df: pd.DataFrame
                DataFrame con la columna de score final.
            - score_column: str
                Nombre de la columna que contiene el score final.
        
        Retorna:
            - pd.DataFrame
                DataFrame con una nueva columna 'Business_Category' que contiene las categorías asignadas.
        """
        if score_column not in df.columns:
            raise KeyError(f"La columna '{score_column}' no existe en el DataFrame.")
        
        ## Activar en caso de querer calcular clientes nuevos
        # Identificar clientes nuevos
        df['IsNew'] = (
            (df['MonthsWithPurchases'] == 1) & 
            (df['LastPurchaseDate'].dt.to_period('M') == self.end_date.to_period('M'))
        )
        df.loc[df['IsNew'], 'Business_Category'] = 'Nuevo'
        
        def categorize(score):
            for category, values in self.business_categories.items():
                if str(score) in values:
                    return category
            return "Sin Categoría"

        # Activar si no hay que calcular Nuevos y todo viene del YAML
        #df['Business_Category'] = df[score_column].apply(categorize)

        ## Activar en caso de querer calcular clientes nuevos
        # Asignar categorías solo si no son "Nuevo"
        df.loc[~df['IsNew'], 'Business_Category'] = df.loc[~df['IsNew'], score_column].apply(categorize)
        # Eliminar columna auxiliar
        df.drop(columns=['IsNew'], inplace=True)

        return df

    def process_rfm(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Procesa el cálculo completo del RFM, incluyendo el cálculo del score final y la asignación de categorías.
        
        Parámetros:
            - df: pd.DataFrame
                DataFrame con columnas de puntajes RFM ('Recency_Score', 'Frequency_Score', 'Monetary_Score').
        
        Retorna:
            - pd.DataFrame
                DataFrame con el score final y las categorías de negocio asignadas.
        """
        print("Calculando el score final...")
        df['Final_Score'] = self.calculate_final_score(df)
        print("Asignando categorías de negocio...")
        df = self.assign_business_categories(df)
        df['CutoffDate'] = self.end_date
        print("Procesamiento RFM completado.")
        
        return df
