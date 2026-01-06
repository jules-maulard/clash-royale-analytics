import pandas as pd
import matplotlib.pyplot as plt

def get_shuffle_lines(row):
    """
    Détermine le nombre de lignes transférées sur le réseau.
    - Si Combiner ON et actif (>0) : c'est le Combine Output.
    - Sinon (OFF ou Combiner inactif) : c'est le Map Output.
    """
    if row['Combiner'] == 'ON' and row['Combine Output'] > 0:
        return row['Combine Output']
    return row['Map Output']

def generate_shuffle_lines_graph(csv_file_path, job_name_keyword, output_filename):
    # Load dataset
    df = pd.read_csv(csv_file_path, sep=';')

    # Filter by Job Name
    df_filtered = df[df['Job Name'].str.contains(job_name_keyword, case=False, regex=False)].copy()

    # Filter by MinSize=8
    df_filtered = df_filtered[df_filtered['MinSize'] == 8]

    # Calculate Shuffle Lines (Network Load in Lines)
    df_filtered['Shuffle Lines'] = df_filtered.apply(get_shuffle_lines, axis=1)
    
    # Ensure Map Input is integer
    df_filtered['Map Input'] = df_filtered['Map Input'].astype(int)

    # Pivot table with 'mean' aggregation to handle duplicate Map Input sizes
    pivot_df = df_filtered.pivot_table(index='Map Input', columns='Combiner', values='Shuffle Lines', aggfunc='mean')
    
    # Sort index
    pivot_df = pivot_df.sort_index()

    # Plotting
    plt.figure(figsize=(10, 6))
    
    for column in pivot_df.columns:
        plt.plot(pivot_df.index, pivot_df[column], marker='o', label=f'Combiner {column}')

    plt.title(f'Network Load (Lines): {job_name_keyword} (MinSize=8)')
    plt.xlabel('Map Input (Lines)')
    plt.ylabel('Shuffle Output (Lines)')
    plt.legend()
    plt.grid(True)

    # Save output
    plt.savefig(output_filename)
    plt.close()

if __name__ == "__main__":
    csv_path = "../benchmark_summary.csv"
    
    generate_shuffle_lines_graph(csv_path, "Data Cleaning", "graph_network_cleaning.png")
    generate_shuffle_lines_graph(csv_path, "Nodes & Edges", "graph_network_nodes_edges.png")