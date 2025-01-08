import os

def save_cleaned_data_to_parquet(cleaned_data: dict, output_dir="cleaned_data"):
    """
    Save cleaned data to Parquet files.
    :param cleaned_data: Dictionary of DataFrames, keyed by table name.
    :param output_dir: Directory where Parquet files will be saved.
    """
    os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist
    for table_name, df in cleaned_data.items():
        file_path = os.path.join(output_dir, f"{table_name}_cleaned.parquet")
        df.to_parquet(file_path, index=False)
        print(f"Saved {table_name} cleaned data to: {file_path}")

# Example usage:
if __name__ == "__main__":
    from data_preprocessing import preprocess_all_data
    from data_extraction import extract_all_data

    # Step 1: Extract raw data
    raw_data = extract_all_data()

    # Step 2: Preprocess data
    clean_data = preprocess_all_data(raw_data)

    # Step 3: Save preprocessed data to Parquet files
    save_cleaned_data_to_parquet(clean_data, output_dir="cleaned_data_parquet")
