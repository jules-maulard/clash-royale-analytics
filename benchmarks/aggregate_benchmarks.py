import sys
import re
import glob
import pandas as pd
import os

def extract_metric(text, regex):
    match = re.search(regex, text)
    return int(match.group(1)) if match else 0

def parse_filename_strict(filename):
    # NOUVELLE LOGIQUE
    # Format 1 (Standard) : bench_DATASET_MINSIZE.log  -> Combiner ON
    # Format 2 (Sans Comb): bench_DATASET_MINSIZE_noCombine.log -> Combiner OFF
    
    clean_name = filename.replace(".log", "")
    parts = clean_name.split("_")
    
    meta = {
        "Dataset": "Unknown", 
        "MinSize": 0, 
        "Combiner": "ON" # Par défaut ON
    }
    
    # On vérifie qu'on a au moins 3 parties (bench, dataset, size)
    if len(parts) >= 3:
        meta["Dataset"] = parts[1]
        try:
            meta["MinSize"] = int(parts[2])
        except ValueError:
            meta["MinSize"] = 0
            
        # Vérification du flag "noCombine" en 4ème position
        if len(parts) >= 4 and "nocombine" in parts[3].lower():
            meta["Combiner"] = "OFF"
            
    else:
        print(f"[WARN] Le fichier '{filename}' ne respecte pas le format 'bench_DATASET_MINSIZE[_noCombine].log'")
        
    return meta

def process_all_logs():
    # Chemin relatif : dossier 'logs' au même niveau que le script
    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    log_pattern = os.path.join(log_dir, "*.log")
    
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

    df = pd.DataFrame(all_jobs_data)
    
    if not df.empty:
        # Tri intelligent
        df = df.sort_values(by=["Dataset", "Combiner", "MinSize", "Job Name"])

    output_file = "benchmark_summary.csv"
    df.to_csv(output_file, index=False, sep=";")
    
    print(f"\n[SUCCESS] Données fusionnées dans '{output_file}'")
    if not df.empty:
        print(df[["Log File", "Combiner", "Job Name", "Shuffle Bytes", "CPU Time (ms)"]].to_string(index=False))

if __name__ == "__main__":
    process_all_logs()