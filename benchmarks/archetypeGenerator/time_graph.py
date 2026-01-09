import pandas as pd
import matplotlib.pyplot as plt

def generate_comparison_graph(csv_file_path, output_filename):
    df = pd.read_csv(csv_file_path, sep=';')

    # MODIFICATION ICI : On filtre intelligemment avec les colonnes
    # 1. On ne garde que les jobs "Nodes & Edges"
    # 2. On veut MinSize = 6
    # 3. On veut Combiner = ON (pour rester cohérent avec ton test précédent)
    df_filtered = df[
        (df['Job Name'].str.contains("Nodes & Edges")) & 
        (df['MinSize'] == 6) & 
        (df['Combiner'] == "ON")
    ].copy()

    def get_type(row):
        # On regarde le nom du fichier log (1ère colonne)
        file_name = str(row.iloc[0]).lower()
        if 'mask' in file_name:
            return 'Mask'
        elif 'recurcif' in file_name:
            return 'Recurcif'
        return None

    df_filtered['Type'] = df_filtered.apply(get_type, axis=1)
    
    # On retire ce qui n'est ni Mask ni Recurcif
    df_filtered = df_filtered.dropna(subset=['Type'])

    if df_filtered.empty:
        print("⚠️ Attention : Aucune donnée trouvée pour MinSize=6 (Mask/Recurcif). Vérifie ton CSV !")
        return

    # On s'assure que Map Input est bien numérique pour un tri correct
    df_filtered['Map Input'] = pd.to_numeric(df_filtered['Map Input'])

    # Pivot pour mettre les Types en colonnes
    pivot_df = df_filtered.pivot_table(index='Map Input', columns='Type', values='Total Time (ms)')
    
    # Tri de l'index par valeur numérique (taille du dataset)
    pivot_df = pivot_df.sort_index()
    
    plt.figure(figsize=(10, 6))
    for column in pivot_df.columns:
        plt.plot(pivot_df.index, pivot_df[column], marker='o', label=f'Version {column}')

    # Titre dynamique
    plt.title(f"Temps d'exécution : Mask vs Recurcif\nNodes & Edges (MinSize=6)")
    plt.xlabel('Map Input (Nombre d\'enregistrements)')
    plt.ylabel('Temps Total (ms)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_filename)
    plt.close()
    
    print(f"✅ Graphique généré : {output_filename}")

if __name__ == "__main__":
    csv_path = "../benchmark_summary.csv" # Vérifie que le chemin est bon par rapport à où tu lances le script
    generate_comparison_graph(csv_path, "graph_nodes_edges_mask_vs_recurcif_size6.png")