import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, min, max, mean, stddev

def analyze_data_quality(filename, folder='data'):
    spark = SparkSession.builder \
        .appName("Analyze Data Quality with PySpark") \
        .getOrCreate()

    file_path = os.path.join(folder, filename)
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Cuenta de registros
    record_count = df.count()
    print(f"Total de registros: {record_count}")

    # Cuenta de registros nulos y duplicados
    null_count = df.select([count(when(isnan(c) | isnull(c), c)).alias(c) for c in df.columns]).collect()
    duplicate_count = df.count() - df.dropDuplicates().count()

    print("\nConteo de registros nulos por columna:")
    for col_name, nulls in zip(df.columns, null_count[0]):
        print(f"{col_name}: {nulls}")

    print(f"\nConteo de registros duplicados: {duplicate_count}")

    # Estadísticas descriptivas (mínimo, máximo, promedio, desviación estándar) para columnas numéricas
    numeric_columns = ['Multa Inicial']
    summary_stats = df.select(numeric_columns).summary("min", "max", "mean", "stddev").collect()

    print("\nEstadísticas descriptivas para columnas numéricas:")
    for stat in summary_stats:
        print(f"{stat['summary']}:")

        for col_name in numeric_columns:
            print(f"  {col_name}: {stat[col_name]}")

    # Identificación de valores atípicos (basado en el rango intercuartil)
    for col_name in numeric_columns:
        q1, q3 = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        outliers = df.filter((col(col_name) < lower_bound) | (col(col_name) > upper_bound)).count()

        print(f"\nValores atípicos en la columna {col_name}: {outliers}")

if __name__ == "__main__":
    filename = 'multas_SECOP.csv'
    analyze_data_quality(filename)
