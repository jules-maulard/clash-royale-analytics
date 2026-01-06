import sys
import re
import glob
import pandas as pd
import os

def extract_metric(text, regex):
    match = re.search(regex, text)
    return int(match.group(1)) if match else 0

def parse_filename_strict(filename):
    clean_name = filename.replace(".log", "")
    parts = clean_name.split("_")
    
    meta = {
        "Dataset": "Unknown", 
        "MinSize": 0, 
        "Combiner": "ON"
    }
    
    if len(parts) >= 3:
        meta["Dataset"] = parts[1]
        try:
            meta["MinSize"] = int(parts[2])
        except ValueError:
            meta["MinSize"] = 0
            
        if len(parts) >= 4 and "nocombine" in parts[3].lower():
            meta["Combiner"] = "OFF"
            
    else:
        print(f"[WARN] Le fichier '{filename}' ne respecte pas le format 'bench_DATASET_MINSIZE[_noCombine].log'")
        
    return meta

def process_all_logs():
    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    log_pattern = os.path.join(log_dir, "*.log")
    output_file = "benchmark_summary.csv"
    
    log_files = glob.glob(log_pattern)
    
    if not log_files:
        print(f"Aucun fichier .log trouvé dans : {log_dir}")
        return

    all_jobs_data = []

    for filepath in log_files:
        filename = os.path.basename(filepath)
        
        with open(filepath, 'r') as f:
            content = f.read()

        file_meta = parse_filename_strict(filename)

        jobs_raw = content.split(">>> Starting Job:")
        
        for job_data in jobs_raw[1:]:
            job_name_match = re.match(r'\s*(.*)', job_data)
            job_name = job_name_match.group(1).strip() if job_name_match else "Unknown"

            metrics = {
                "Log File": filename,
                "Dataset": file_meta["Dataset"],
                "MinSize": file_meta["MinSize"],
                "Combiner": file_meta["Combiner"],
                "Job Name": job_name,
                "Map Input": extract_metric(job_data, r"Map input records=(\d+)"),
                "Map Output": extract_metric(job_data, r"Map output records=(\d+)"),
                "Combine Output": extract_metric(job_data, r"Combine output records=(\d+)"),
                "Shuffle Bytes": extract_metric(job_data, r"Reduce shuffle bytes=(\d+)"),
                "CPU Time (ms)": extract_metric(job_data, r"CPU time spent \(ms\)=(\d+)"),
                "Total Time (ms)": extract_metric(job_data, r"Total time spent by all maps in occupied slots \(ms\)=(\d+)"),
            }
            all_jobs_data.append(metrics)

    new_df = pd.DataFrame(all_jobs_data)
    
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file, sep=";")
            if not new_df.empty:
                processed_files = new_df["Log File"].unique()
                existing_df = existing_df[~existing_df["Log File"].isin(processed_files)]
                
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
        except pd.errors.EmptyDataError:
            final_df = new_df
    else:
        final_df = new_df

    if not final_df.empty:
        final_df = final_df.sort_values(by=["Dataset", "Combiner", "MinSize", "Job Name"])
        final_df.to_csv(output_file, index=False, sep=";")
        
        print(f"\n[SUCCESS] Données mises à jour dans '{output_file}'")
        print(final_df[["Log File", "Combiner", "Job Name", "Shuffle Bytes", "CPU Time (ms)"]].tail(10).to_string(index=False))

if __name__ == "__main__":
    process_all_logs()