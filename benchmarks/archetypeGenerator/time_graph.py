import pandas as pd
import matplotlib.pyplot as plt

def generate_comparison_graph(csv_file_path, output_filename):
    df = pd.read_csv(csv_file_path, sep=';')

    target_job = "Nodes & Edges [Combiner=ON, MinSize=8]"
    df_filtered = df[df['Job Name'] == target_job].copy()

    def get_type(row):
        file_name = str(row.iloc[0]).lower()
        if '_mask' in file_name:
            return 'Mask'
        elif '_recurcif' in file_name:
            return 'Recurcif'
        return None

    df_filtered['Type'] = df_filtered.apply(get_type, axis=1)
    df_filtered = df_filtered.dropna(subset=['Type'])

    # On s'assure que Map Input est bien numérique pour un tri correct
    df_filtered['Map Input'] = pd.to_numeric(df_filtered['Map Input'])

    # Pivot direct
    pivot_df = df_filtered.pivot_table(index='Map Input', columns='Type', values='Total Time (ms)')
    
    # Tri de l'index par valeur numérique
    pivot_df = pivot_df.sort_index()
    
    plt.figure(figsize=(10, 6))
    for column in pivot_df.columns:
        plt.plot(pivot_df.index, pivot_df[column], marker='o', label=f'Version {column}')

    plt.title(f'Temps d\'exécution : Mask vs Recurcif\n{target_job}')
    plt.xlabel('Map Input (Taille)')
    plt.ylabel('Temps Total (ms)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_filename)
    plt.close()

if __name__ == "__main__":
    csv_path = "../benchmark_summary.csv"
    generate_comparison_graph(csv_path, "graph_nodes_edges_mask_vs_recurcif.png")