import pandas as pd
import matplotlib.pyplot as plt

def convert_dataset_size(size_str):
    """Converts '100k', '1M' into integers."""
    size_str = size_str.lower()
    if 'm' in size_str:
        return int(float(size_str.replace('m', '')) * 1_000_000)
    elif 'k' in size_str:
        return int(float(size_str.replace('k', '')) * 1_000)
    return int(size_str)

def generate_execution_time_graph(csv_file_path, job_name_keyword, output_filename):
    df = pd.read_csv(csv_file_path, sep=';')

    # Filter rows based on Job Name
    df_filtered = df[df['Job Name'].str.contains(job_name_keyword, case=False, regex=False)].copy()
    df_filtered = df_filtered[df_filtered['MinSize'] == 8]

    # Calculate real numeric value for sorting
    df_filtered['Dataset_Val'] = df_filtered['Dataset'].apply(convert_dataset_size)

    # Pivot data
    pivot_df = df_filtered.pivot(index='Dataset', columns='Combiner', values='Total Time (ms)')

    # Reorder the index based on the calculated Dataset_Val
    # We create a mapping from Dataset name to Value, sort it, and reindex the pivot
    sort_order = df_filtered[['Dataset', 'Dataset_Val']].drop_duplicates().sort_values('Dataset_Val')['Dataset']
    pivot_df = pivot_df.reindex(sort_order)

    # Plotting
    plt.figure(figsize=(10, 6))
    
    for column in pivot_df.columns:
        plt.plot(pivot_df.index, pivot_df[column], marker='o', label=f'Combiner {column}')

    plt.title(f'Execution Time: {job_name_keyword} (MinSize=8)')
    plt.xlabel('Dataset Size')
    plt.ylabel('Total Time (ms)')
    plt.legend()
    plt.grid(True)

    plt.savefig(output_filename)
    plt.close()

if __name__ == "__main__":
    csv_path = "../benchmark_summary.csv"
    generate_execution_time_graph(csv_path, "Data Cleaning", "graph_combine_time_cleaning.png")
    generate_execution_time_graph(csv_path, "Nodes & Edges", "graph_combine_time_nodes_edges.png")