"""
Proyecto: Demo RFM
Módulo: rfm_calculator.py
Versión: 1.0
Fecha de creación: 2024-12-10
Autor: Carolina Torres Zapata
Modificado por: 
Fecha modificación: 
Descripción: 
    Este módulo contiene la clase `RFMCalculator`, que implementa los métodos necesarios para calcular las 
    métricas RFM (Recency, Frequency, Monetary) para un conjunto de datos de transacciones de clientes. 
    
"""

### Importar Librerías
import pandas as pd
from modules.data_loader import DataLoader


class RFMCalculator:
    def __init__(self, config_path: str):
        """
        Inicializa el calculador RFM con la configuración especificada en un archivo YAML.
        
        Este constructor carga la configuración desde el archivo YAML y obtiene la configuración de las 
        columnas necesarias para calcular las métricas RFM. También obtiene el rango de fechas para realizar 
        el análisis RFM a través del `DataLoader`.

        Parámetros:
            - config_path: str
                Ruta del archivo YAML que contiene la configuración del cálculo RFM.
                El archivo YAML debe incluir configuraciones globales para las columnas de los datos y 
                el rango de fechas necesario para calcular RFM.

            - self.data_loader: DataLoader
                Instancia de la clase `DataLoader` que se utiliza para cargar y manipular los datos del archivo 
                de configuración.

            - self.start_date: datetime
                Fecha de inicio para el análisis RFM, obtenida a través del `DataLoader`.
            
            - self.end_date: datetime
                Fecha de fin para el análisis RFM, también obtenida a través del `DataLoader`.
        """
        
        # Cargar configuración desde YAML
        self.config = DataLoader.load_config(config_path)
        
        # Obtener configuraciones de columnas
        columns_config = self.config.get("global_settings", {}).get("columns", {})
        self.columns = {
            "customer_id": columns_config.get("customer_id", "CustomerID"),
            "date": columns_config.get("date", "InvoiceDate"),
            "invoice": columns_config.get("invoice", "InvoiceNo"),
            "price": columns_config.get("price", "UnitPrice"),
            "quantity": columns_config.get("quantity", "Quantity"),
        }
        
        # Rango de fechas para el análisis
        self.data_loader = DataLoader(config_path=config_path)
        self.start_date, self.end_date = self.data_loader.get_date_range_for_rfm()


    def calculate_rfm(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula los valores RFM (Recency, Frequency, Monetary) para cada cliente a partir de los datos de transacciones.

        El cálculo RFM se realiza agrupando las transacciones por cliente y luego aplicando las siguientes métricas:
        - **Recency**: El número de días desde la última compra de cada cliente hasta la fecha final del análisis.
        - **Frequency**: El número de compras únicas realizadas por cada cliente.
        - **Monetary**: El total gastado por cada cliente, sumando el valor de todas sus compras.
        - **LastPurchaseDate**: La fecha de la última compra realizada por el cliente.
        - **MonthsWithPurchases**: El número de meses en los que el cliente realizó al menos una compra.

        Parámetros:
            - data: pd.DataFrame
                Un DataFrame que contiene los datos de las transacciones, con al menos las siguientes columnas:
                - **customer_id**: Identificador único para cada cliente.
                - **date**: Fecha de la transacción.
                - **price**: Monto de la transacción.
                - **invoice**: Identificador de la factura (usado para contar la frecuencia de compras).

        Retorna:
            - pd.DataFrame
                Un DataFrame con los valores calculados de RFM para cada cliente, donde las columnas son:
                - **customer_id**: El identificador del cliente.
                - **Recency**: La cantidad de días desde la última compra.
                - **Frequency**: El número de compras realizadas por el cliente.
                - **Monetary**: El total gastado por el cliente.
                - **LastPurchaseDate**: La fecha de la última compra.
                - **MonthsWithPurchases**: El número de meses en los que el cliente realizó compras.

        Excepciones:
            - KeyError: Si alguna de las columnas necesarias (`customer_id`, `date`, `price`) no se encuentra en el DataFrame proporcionado.
        
        """
        required_columns = [
            self.columns["customer_id"],
            self.columns["date"],
            self.columns["price"],
        ]
        for col in required_columns:
            if col not in data.columns:
                raise KeyError(f"La columna requerida '{col}' no se encuentra en el DataFrame.")
        
        customer_col = self.columns["customer_id"]
        date_col = self.columns["date"]
        price_col = self.columns["price"]
        invoice_col = self.columns["invoice"]

        # Asegurar formato datetime en la columna de fechas
        data[date_col] = pd.to_datetime(data[date_col])
        
        # Realizar todas las agregaciones en una sola llamada groupby
        rfm_data = data.groupby(customer_col).agg(
            Recency=(date_col, lambda x: (self.end_date - x.max()).days),
            Frequency=(date_col, 'nunique'),
            Monetary=(price_col, 'sum'),
            LastPurchaseDate=(date_col, 'max'),
            MonthsWithPurchases=(date_col, lambda x: x.dt.to_period('M').nunique())
        ).reset_index()

        return rfm_data

