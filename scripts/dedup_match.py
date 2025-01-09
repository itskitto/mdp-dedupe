import pandas as pd
import dedupe
import os

from scripts.common import load_config

config = load_config()
PARQUET_DIR = os.path.join(config.get("paths.processed_data"), "parquet")
RESULTS_DIR = config.get("paths.results")
SETTINGS_FILE = os.path.join(config.get("paths.models"), "dedupe_model_settings")
os.makedirs(RESULTS_DIR, exist_ok=True)

def get_dedupe_fields():
    """Define the fields for Dedupe."""
    return [
        dedupe.variables.String("first_name"),
        dedupe.variables.String("last_name"),
        dedupe.variables.String("email"),
        dedupe.variables.String("phone_number"),
        dedupe.variables.String("address"),
        dedupe.variables.String("date_of_birth"),
    ]

def prepare_dedupe_data(dataframes):
    """Combine all records into a format suitable for Dedupe."""
    combined_data = {}
    for source, df in dataframes.items():
        for idx, row in df.iterrows():
            record_id = f"{source}_{row['id']}" 
            combined_data[record_id] = row.to_dict()
    return combined_data

def setup_dedupe(dedupe_fields, training_data):
    """Set up the Dedupe model."""
    if os.path.exists(SETTINGS_FILE):
        print(f"Loading pre-trained settings from {SETTINGS_FILE}...")
        with open(SETTINGS_FILE, "rb") as sf:
            deduper = dedupe.StaticDedupe(sf)
    else:
        print("Training a new Dedupe model...")
        deduper = dedupe.Dedupe(dedupe_fields)
        deduper.prepare_training(training_data)

        # Active learning: label pairs of records
        print("Starting active labeling...")
        dedupe.console_label(deduper)

        # Train the model
        deduper.train()

        # Save model settings
        with open(SETTINGS_FILE, "wb") as sf:
            deduper.write_settings(sf)

    return deduper

def deduplicate_records(deduper, data):
    """Cluster duplicate records."""
    print("Clustering records...")
    return deduper.partition(data, threshold=0.5)

def save_dedupe_results(clusters, output_path):
    """Save deduplication results to a file."""
    print(f"Saving deduplication results to {output_path}...")
    with open(output_path, "w") as f:
        for cluster_id, (records, score) in enumerate(clusters):
            for record_id in records:
                f.write(f"{cluster_id},{record_id},{score}\n")

if __name__ == "__main__":
    dataframes = {}
    for source in ["clinic", "urgent_care", "hospital", "physical_therapy"]:
        path = os.path.join(PARQUET_DIR, f"{source}_preprocessed.parquet")
        if os.path.exists(path):
            print(f"Loading {source} data from {path}...")
            dataframes[source] = pd.read_parquet(path)
        else:
            print(f"Warning: {path} does not exist!")

    dedupe_data = prepare_dedupe_data(dataframes)

    fields = get_dedupe_fields()
    deduper = setup_dedupe(fields, dedupe_data)

    clusters = deduplicate_records(deduper, dedupe_data)
    print(f"Found {len(clusters)} clusters of duplicates.")

    output_path = os.path.join(RESULTS_DIR, "dedupe_results.csv")
    save_dedupe_results(clusters, output_path)
