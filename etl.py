#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
from pyspark.sql.functions import countDistinct
from pyspark.sql import functions as F


import lectura_paco_colusiones
import lectura_paco_multas
import lectura_paco_secop
import pyspark_etl_functions


# In[4]:


url = 'https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/colusiones_en_contratacion_SIC.csv'
filename = 'colusiones_en_contratacion_SIC.csv'
lectura_paco_colusiones.download_and_save_csv(url, filename)


# In[5]:


url = 'https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/multas_SECOP.csv'
filename = 'multas_SECOP.csv'
lectura_paco_multas.download_and_save_csv(url, filename)


# In[6]:


url = 'https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/SECOP_II_Cleaned.csv'
filename = 'SECOP_II_Cleaned.csv'
lectura_paco_secop.download_and_save_csv(url, filename)


# # Quality PACO_secop

# In[7]:


filename = 'SECOP_II_Cleaned.csv'
paco_secop_df = pyspark_etl_functions.read_csv_with_pyspark(filename, folder='data', separator='|')
paco_secop_df.show(5)


# In[8]:


# pyspark_etl_functions.analyze_data_quality(paco_secop_df)


# In[9]:


nombre_familia_df = paco_secop_df.select('NOMBRE_FAMILIA').distinct().toPandas()
pd.set_option('display.max_rows', None) 
print(nombre_familia_df)


# - VIVIENDA CIUDAD Y TERRITORIO: Se relaciona con la construcción y desarrollo de viviendas, urbanismo y ordenamiento territorial.
# - TRANSPORTE: Incluye la infraestructura vial, ferroviaria, aeroportuaria y marítima, así como la construcción y mantenimiento de medios de transporte.
# - MINAS Y ENERGÍA: Abarca la infraestructura relacionada con la extracción de recursos minerales y la producción y distribución de energía.
# - AMBIENTE Y DESARROLLO SOSTENIBLE: Puede incluir infraestructuras sostenibles y proyectos relacionados con el medio ambiente, como plantas de tratamiento de agua y sistemas de gestión de residuos.
# 

# In[10]:


infraestructura_categorias = [
    "VIVIENDA CIUDAD Y TERRITORIO",
    "TRANSPORTE",
    "MINAS Y ENERGIA",
    "AMBIENTE Y DESARROLLO SOSTENIBLE"
]

infraestructura_df = paco_secop_df.filter(paco_secop_df.NOMBRE_FAMILIA.isin(infraestructura_categorias))

infraestructura_df.show()


# In[11]:


pyspark_etl_functions.analyze_data_quality(infraestructura_df)


# In[12]:


distinct_entity_names = infraestructura_df.groupBy("ID_CONTRATISTA").agg(
    countDistinct("RAZON_SOCIAL_CONTRATISTA").alias("cantidad_razon_social_distintos")
)
 
distinct_entity_names.orderBy("cantidad_razon_social_distintos", ascending=False).show()


# In[13]:


nit_filtered_df = infraestructura_df.filter(infraestructura_df.ID_CONTRATISTA == 899999061)

distinct_nombre_entidad = nit_filtered_df.select("RAZON_SOCIAL_CONTRATISTA").distinct()

nombre_entidad_rows = distinct_nombre_entidad.collect()

nombre_entidad_list = [row.RAZON_SOCIAL_CONTRATISTA for row in nombre_entidad_rows]

print(nombre_entidad_list)


# ## ETL

# In[14]:


columnas_referencia = ['REFERENCIA_CONTRATO','ID_CONTRATISTA']
infraestructura_df_limpio = pyspark_etl_functions.limpiar_nulos_y_duplicados(paco_secop_df, columnas_referencia)
infraestructura_df_limpio.show()


# In[15]:


infraestructura_df_limpio_2_anos = pyspark_etl_functions.filtrar_ultimos_anos(infraestructura_df_limpio, 2)
infraestructura_df_limpio_2_anos.show()


# In[16]:


columnas_a_conservar = [
    "REFERENCIA_CONTRATO", "MUNICIPIO", "DEPARTAMENTO", "ESTADO_DEL_PROCESO",
    "FECHA_INICIO_CONTRATO", "FECHA_FIN_CONTRATO", "CLASE_PROCESO", "TIPO_PROCESO",
    "TIPO_CONTRATO", "NOMBRE_ENTIDAD", "NIT_ENTIDAD", "ID_CONTRATISTA", "RAZON_SOCIAL_CONTRATISTA",
    "VALOR_TOTAL_CONTRATO", "NOMBRE_GRUPO", "NOMBRE_FAMILIA", "NOMBRE_CLASE", "month", "year"
]

infraestructura_df_seleccionado = pyspark_etl_functions.seleccionar_columnas(infraestructura_df_limpio_2_anos, columnas_a_conservar)
infraestructura_df_seleccionado.show()


# In[17]:


infraestructura_df_seleccionado.write.csv("etl_data/contratos_infraesructura_df.csv", mode="overwrite", header=True)


# In[18]:


infraestructura_df_agregado = pyspark_etl_functions.agregar_por_id_contratista(infraestructura_df_seleccionado)
infraestructura_df_agregado.show()


# In[19]:


infraestructura_df_pivotado = pyspark_etl_functions.pivotar_por_columna(infraestructura_df_seleccionado, "DEPARTAMENTO")
infraestructura_df_pivotado.show()


# In[20]:


infraestructura_df_agregado_y_pivotado = pyspark_etl_functions.unir_dataframes(infraestructura_df_agregado, infraestructura_df_pivotado)
infraestructura_df_agregado_y_pivotado.show()


# In[21]:


infraestructura_df_pivotado = pyspark_etl_functions.pivotar_por_columna(infraestructura_df_seleccionado, "ESTADO_DEL_PROCESO")
infraestructura_df_pivotado.show()


# In[22]:


infraestructura_df_agregado_y_pivotado = pyspark_etl_functions.unir_dataframes(infraestructura_df_agregado_y_pivotado, infraestructura_df_pivotado)
infraestructura_df_agregado_y_pivotado.show()


# In[23]:


infraestructura_df_pivotado = pyspark_etl_functions.pivotar_por_columna(infraestructura_df_seleccionado, "CLASE_PROCESO")
infraestructura_df_pivotado.show()


# In[24]:


infraestructura_df_agregado_y_pivotado = pyspark_etl_functions.unir_dataframes(infraestructura_df_agregado_y_pivotado, infraestructura_df_pivotado)
infraestructura_df_agregado_y_pivotado.show()


# In[25]:


infraestructura_df_pivotado = pyspark_etl_functions.pivotar_por_columna(infraestructura_df_seleccionado, "TIPO_PROCESO")
infraestructura_df_pivotado.show()


# In[26]:


infraestructura_df_agregado_y_pivotado = pyspark_etl_functions.unir_dataframes(infraestructura_df_agregado_y_pivotado, infraestructura_df_pivotado)
infraestructura_df_agregado_y_pivotado.show()


# In[27]:


infraestructura_df_pivotado = pyspark_etl_functions.pivotar_por_columna(infraestructura_df_seleccionado, "NOMBRE_FAMILIA")
infraestructura_df_pivotado.show()


# In[28]:


infraestructura_df_agregado_y_pivotado = pyspark_etl_functions.unir_dataframes(infraestructura_df_agregado_y_pivotado, infraestructura_df_pivotado)
infraestructura_df_agregado_y_pivotado.show()


# In[29]:


infraestructura_df_pivotado = pyspark_etl_functions.pivotar_por_columna(infraestructura_df_seleccionado, "NOMBRE_CLASE")
infraestructura_df_pivotado.show()


# In[30]:


infraestructura_df_agregado_y_pivotado = pyspark_etl_functions.unir_dataframes(infraestructura_df_agregado_y_pivotado, infraestructura_df_pivotado)
infraestructura_df_agregado_y_pivotado.show()


# In[31]:


infraestructura_df_ordenado = infraestructura_df_agregado_y_pivotado.orderBy(F.desc("num_contratos"))

infraestructura_df_ordenado.show()


# In[32]:


def separar_dataframe(df):
    df_cero = df.filter(df.suma_valor_total_contrato == 0)
    df_no_cero = df.filter(df.suma_valor_total_contrato != 0)
    return df_cero, df_no_cero


# In[33]:


infraestructura_df_cero, infraestructura_df_no_cero = separar_dataframe(infraestructura_df_ordenado)

print("Dataframe con suma_valor_total_contrato = 0:")
infraestructura_df_no_cero.show()   


# In[34]:


num_filas_cero = infraestructura_df_cero.count()
num_filas_no_cero = infraestructura_df_no_cero.count()

print("Número de filas con suma_valor_total_contrato = 0:", num_filas_cero)
print("Número de filas con suma_valor_total_contrato != 0:", num_filas_no_cero)


# # Quality Colusiones

# In[35]:


# filename = 'colusiones_en_contratacion_SIC.csv'
# colusiones_df = pyspark_etl_functions.read_csv_with_pyspark(filename, folder='data', separator=',')
# colusiones_df.show(5)


# In[36]:


# def agg_colusiones(colusiones_df):
#     # Calcular los meses desde el último Año Radicacion hasta la fecha actual
#     current_year = 2023
#     colusiones_df = colusiones_df.withColumn("Meses_desde_Ultimo_Año", (current_year - F.col("Año Radicacion")) * 12)
    
#     # Agrupar por Identificacion y calcular las métricas solicitadas
#     colusiones_agg = (
#         colusiones_df.groupBy("Identificacion")
#         .agg(
#             F.count("*").alias("num_colusiones"),
#             F.sum("Multa Inicial").alias("suma_multa_inicial"),
#             F.avg("Multa Inicial").alias("promedio_multa_inicial"),
#             F.max("Meses_desde_Ultimo_Año").alias("meses_desde_ultima_colusion")
#         )
#     )
    
#     return colusiones_agg


# In[37]:


# colusiones_agg_df = agg_colusiones(colusiones_df)
# colusiones_agg_df.show(5)


# In[38]:


# def left_join_dataframes(df1, df2, df1_key, df2_key):
#     joined_df = df1.join(df2, df1[df1_key] == df2[df2_key], how="left")
#     return joined_df

# # Realizar el left join
# result_df = left_join_dataframes(infraestructura_df_cero, colusiones_agg_df, "NIT_ENTIDAD", "Identificacion")
# # result_df.show(5)
# result_df.orderBy(F.desc("Identificacion")).show(5)


# # Quality multas

# In[39]:


filename = 'multas_SECOP.csv'
multas_df = pyspark_etl_functions.read_csv_with_pyspark(filename, folder='data', separator=',')
multas_df.show(5)


# In[40]:


pyspark_etl_functions.analyze_data_quality(multas_df)


# In[41]:


# Read the data and process it using the aggregate_multas_data function
aggregated_multas = pyspark_etl_functions.aggregate_multas_data(multas_df)
aggregated_multas = aggregated_multas.withColumnRenamed("documento_contratista", "documento_contratista_multas")
aggregated_multas.show(5)


# In[42]:


result_df = pyspark_etl_functions.left_join_dataframes(infraestructura_df_no_cero, aggregated_multas, "ID_CONTRATISTA", "documento_contratista_multas")


# In[43]:


# Check the joined dataframe
print("Joined dataframe:")
result_df.orderBy(F.desc("numero_de_multas")).show(5)
# result_df.show(5)


# In[44]:


result_df = result_df.drop("documento_contratista_multas")


# In[45]:


from collections import Counter

column_counts = Counter(result_df.columns)

duplicate_columns = [col for col, count in column_counts.items() if count > 1]

result_df = result_df.drop(*duplicate_columns)


# In[46]:


result_df.write.csv("etl_data/contratistas_df.csv", mode="overwrite", header=True)


# In[47]:


# jupyter nbconvert --to script etl.ipynb


# import subprocess
# subprocess.run(["python3", "nombre.py"])



# def my_function():
#     print("Hello, World!")

# if __name__ == "__main__":
#     my_function()

# import nombre

# nombre.my_function()


