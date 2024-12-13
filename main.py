from modules.data_loader import DataLoader
from modules.preprocessing import DataPreprocessor
from modules.rfm_calculator import RFMCalculator
from modules.rfm_processing import RFMProcessing
from modules.segment_assigner import RFMProcessor
from modules.exporter import DataExporter
import pandas as pd
import os

if __name__ == "__main__":
    
    # Ruta al archivo de configuración en la carpeta 'config'
    config_path = os.path.join("config", "configuracion.yaml")  

    try:
        # Instancia de DataLoader con el archivo de configuración
        data_loader = DataLoader(config_path)
        
        # Instancia de DataPreprocessor con el archivo de configuración
        preprocessor = DataPreprocessor(config_path)

        # Instancia de RFM Calculator con el archivo de configuración
        rfm_calculator = RFMCalculator(config_path)

        # Instancia de RFMProcessing con el archivo de configuración
        rfm_processor = RFMProcessing(config_path)

        # Instancia de RFMProcessor con el archivo de configuración
        rfm_assigner = RFMProcessor(config_path)

        # Instancia de DataExporter con el archivo de configuración
        exporter = DataExporter(config_path)

        # Cargar datos desde la fuente específica de Excel
        data = data_loader.load_from_excel(excel_key ='retail_data', filter_dates = True) 

        # Preprocesamiento de datos
        data_processed = preprocessor.apply_preprocessing_to_source(data, "retail_data")

        # Mostrar los primeros registros del DataFrame procesado
        print("\nDatos después del preprocesamiento:")
        print(data_processed.head())

        # Calcular RFM usando los datos procesados y la configuración cargada
        rfm_data = rfm_calculator.calculate_rfm(data_processed)
        # # Mostrar los resultados de RFM
        print("\nResultados del cálculo de RFM:")
        print(rfm_data.head())

        # Calular LS, LI, Breaks y Puntaje RFM
        df_resultado = rfm_processor.process_rfm_data(rfm_data)
        print("\nResultados del Puntaje RFM:")
        print(df_resultado.head())

         # Calcular el score final 
        rfm_result = rfm_assigner.process_rfm(df_resultado)
        print("\nResultados del Puntaje RFM Total:")
        print(rfm_result.head())

        # Exportar el resultado a un archivo CSV
        exporter.export_to_csv(rfm_result, "results_csv")

      
    except Exception as e:
        print(f"Error: {e}")

