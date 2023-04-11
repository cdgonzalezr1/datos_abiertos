import os
import pandas as pd

def obtener_datos_contratacion_publica(url: str, fecha_desde: str = None) -> pd.DataFrame:
    try:
        if fecha_desde:
            url += f"&$where=fecha_de_firma_del_contrato>='{fecha_desde}'"
        
        datos = pd.read_csv(url)
        return datos
    except Exception as e:
        print(f"Error al leer los datos: {e}")
        return None

def guardar_datos_en_csv(datos: pd.DataFrame, archivo: str):
    try:
        datos.to_csv('data/' + archivo, index=False)
    except Exception as e:
        print(f"Error al guardar los datos en el archivo CSV: {e}")

def main():
    archivo_csv = "contratacion_publica_colombia.csv"
    url_datos = "https://www.datos.gov.co/resource/qau8-txut.csv?$limit=500000"
    
    fecha_desde = None
    if os.path.exists(archivo_csv):
        datos_existentes = pd.read_csv(archivo_csv)
        fecha_desde = datos_existentes["fecha_de_firma_del_contrato"].max()
    else:
        datos_existentes = pd.DataFrame()

    nuevos_datos = obtener_datos_contratacion_publica(url_datos, fecha_desde)
    
    if nuevos_datos is not None and not nuevos_datos.empty:
        datos_combinados = pd.concat([datos_existentes, nuevos_datos], ignore_index=True)
        guardar_datos_en_csv(datos_combinados, archivo_csv)
        print("Se han actualizado los datos en el archivo CSV.")
    else:
        print("No se encontraron nuevos registros.")

if __name__ == "__main__":
    main()
