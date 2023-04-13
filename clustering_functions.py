import os
import pandas as pd
from sklearn.preprocessing import StandardScaler
import numpy as np
import umap
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt


def read_partitioned_csv(path):
    all_files = []
    
    for dirname, _, filenames in os.walk(path):
        for filename in filenames:
            if filename.endswith(".csv"):
                file_path = os.path.join(dirname, filename)
                all_files.append(file_path)
    
    dfs = [pd.read_csv(file) for file in all_files]
    combined_df = pd.concat(dfs, ignore_index=True)
    
    return combined_df


def replace_nulls(df):
    df['meses_desde_ultima_multa'] = df['meses_desde_ultima_multa'].fillna(99999999)

    columns_to_replace = df.columns[df.columns != 'meses_desde_ultima_multa']
    df[columns_to_replace] = df[columns_to_replace].fillna(0)

    return df



def apply_standard_scaler(df):
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df)
    
    scaled_df = pd.DataFrame(scaled_data, columns=df.columns)
    
    return scaled_df





def apply_umap(df, n_components):
    umap_model = umap.UMAP(n_components=n_components, random_state=42)
    umap_data = umap_model.fit_transform(df)
    
    plt.scatter(umap_data[:, 0], umap_data[:, 1], s=5)
    plt.xlabel("Component 1")
    plt.ylabel("Component 2")
    plt.title("UMAP Visualization")
    plt.show()
    
    explained_variance = np.var(umap_data, axis=0)
    explained_variance_ratio = explained_variance / np.sum(explained_variance)
    explained_variance_ratio_cumsum = np.cumsum(explained_variance_ratio)
    
    return umap_data, explained_variance_ratio_cumsum




def find_optimal_clusters(data, max_clusters=10):
    wcss = []
    silhouette = []
    clusters_range = range(2, max_clusters+1)

    for n_clusters in clusters_range:
        kmeans = KMeans(n_clusters=n_clusters, init="k-means++", random_state=42)
        kmeans.fit(data)
        wcss.append(kmeans.inertia_)
        silhouette.append(silhouette_score(data, kmeans.labels_))
    
    plt.plot(clusters_range, wcss, 'bo-')
    plt.xlabel("Number of Clusters")
    plt.ylabel("WCSS")
    plt.title("Elbow Plot")
    plt.show()

    plt.plot(clusters_range, silhouette, 'bo-')
    plt.xlabel("Number of Clusters")
    plt.ylabel("Silhouette Score")
    plt.title("Silhouette Plot")
    plt.show()

    optimal_clusters = np.argmax(silhouette) + 2

    kmeans = KMeans(n_clusters=optimal_clusters, init="k-means++", random_state=42)
    kmeans.fit(data)
    labels = kmeans.labels_

    plt.scatter(data[:, 0], data[:, 1], c=labels, cmap='viridis', s=5)
    plt.xlabel("Component 1")
    plt.ylabel("Component 2")
    plt.title("UMAP Components Colored by Cluster")
    plt.show()

    return optimal_clusters, labels




def get_cluster_centroids(dataframe: pd.DataFrame, cluster_column: str = 'cluster', exclude_columns: list = None):
    if exclude_columns is None:
        exclude_columns = [cluster_column, "NOMBRE_ENTIDAD"]
    else:
        exclude_columns.extend([cluster_column, "NOMBRE_ENTIDAD"])

    centroid_columns = [col for col in dataframe.columns if col not in exclude_columns]

    centroids = dataframe.groupby(cluster_column)[centroid_columns].mean()

    return centroids







