import streamlit as st
import pandas as pd

def main():
    st.title("Dashboard: Índice de Transparencia de la contratación pública en infraestructura")

    data = pd.read_csv("anomaly_data/contratos_indice_transparencia.csv")

    # Filtros
    st.sidebar.title("Filtros")
    departments = st.sidebar.multiselect("Selecciona Departamentos", data['DEPARTAMENTO'].unique())
    entities = st.sidebar.multiselect("Selecciona Entidades", data['NOMBRE_ENTIDAD'].unique())
    families = st.sidebar.multiselect("Selecciona Familias", data['NOMBRE_FAMILIA'].unique())
    years = st.sidebar.multiselect("Selecciona Años", data['year'].unique())

    filtered_data = data.copy()

    if departments:
        filtered_data = filtered_data[filtered_data['DEPARTAMENTO'].isin(departments)]

    if entities:
        filtered_data = filtered_data[filtered_data['NOMBRE_ENTIDAD'].isin(entities)]

    if families:
        filtered_data = filtered_data[filtered_data['NOMBRE_FAMILIA'].isin(families)]

    if years:
        filtered_data = filtered_data[filtered_data['year'].isin(years)]

    # Calcular el índice de transparencia promedio por departamento
    avg_transparency_by_department = filtered_data.groupby('DEPARTAMENTO')['indice_de_transparencia_del_contrato'].mean()
    avg_transparency_by_department = avg_transparency_by_department.reset_index()

    # Mostrar un gráfico de barras
    st.subheader("Gráfico de Barras: Índice de Transparencia del Contrato Promedio por Departamento")
    chart = st.bar_chart(avg_transparency_by_department.set_index('DEPARTAMENTO'))

    # Calcular el índice de transparencia promedio por TIPO_CONTRATO
    avg_transparency_by_tipo_contrato = filtered_data.groupby('TIPO_CONTRATO')['indice_de_transparencia_del_contrato'].mean()
    avg_transparency_by_tipo_contrato = avg_transparency_by_tipo_contrato.reset_index()

    # Mostrar un gráfico de barras
    st.subheader("Gráfico de Barras: Índice de Transparencia del Contrato Promedio por TIPO_CONTRATO")
    chart = st.bar_chart(avg_transparency_by_tipo_contrato.set_index('TIPO_CONTRATO'))

if __name__ == "__main__":
    main()

