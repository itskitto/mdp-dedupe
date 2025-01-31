"""Data preprocessing script for normalizing and cleaning data."""
import os
from pathlib import Path

import pandas as pd

from mdp_dedupe.scripts.common import load_config

config = load_config()
PARQUET_DIR = Path(config.paths["processed_data"]) / "parquet"

def normalize_text(value):
    """Normalize text fields by stripping whitespace and converting to lowercase.

    Args:
        value: Text value to normalize.

    Returns:
        Normalized text or None if input is null.
    """
    if pd.isnull(value):
        return None
    return str(value).strip().lower()

def normalize_phone(phone):
    """Normalize phone numbers by stripping to digits only.

    Args:
        phone: Phone number to normalize.

    Returns:
        Normalized phone number or None if input is null.
    """
    if pd.isnull(phone):
        return None
    return "".join(filter(str.isdigit, str(phone)))

def normalize_date(date, date_format="%Y-%m-%d"):
    """Normalize dates to a standard format using pandas.to_datetime.

    Args:
        date: Date value to normalize.
        date_format: Output date format (default: YYYY-MM-DD).

    Returns:
        Normalized date string or None if input is invalid.
    """
    if pd.isnull(date):
        return None

    try:
        parsed_date = pd.to_datetime(date, errors="coerce")
        if pd.isnull(parsed_date):
            print(f"Warning: Unable to parse date '{date}'")
            return None

        return parsed_date.strftime(date_format)
    except Exception as e:
        print(f"Error parsing date '{date}': {e}")
        return None

def flatten_address(address):
    """Flatten address fields into a single string.

    Args:
        address: Address value to flatten (can be dict or string).

    Returns:
        Flattened address string or None if input is null.
    """
    if isinstance(address, dict):
        return ", ".join(str(value) for value in address.values() if value)
    if pd.isnull(address):
        return None
    return str(address)

def preprocess_source_data(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Preprocess data for a specific source based on field mappings.

    Args:
        df: Input DataFrame.
        source: Name of the data source.

    Returns:
        Preprocessed DataFrame.
    """
    if "id" not in df.columns:
        df = df.reset_index(drop=True)
        df["id"] = df.index + 1

    field_mappings = config.get(f"field_mappings.{source}", {})
    for source_field, target_field in field_mappings.items():
        if source_field in df.columns:
            df[target_field] = df[source_field]

    if source == "physical_therapy" and "full_name" in df.columns:
        df["first_name"] = df["full_name"].str.extract(r"^(\S+)", expand=False)
        df["last_name"] = df["full_name"].str.extract(r"\s+(.+)$", expand=False)

    for field in df.columns:
        if "date" in field:
            print(f"Normalizing {field} for {source}...")
            df[field] = df[field].apply(normalize_date)
        elif "address" in field:
            print(f"Normalizing {field} for {source}...")
            df[field] = df[field].apply(flatten_address)
        elif "phone" in field:
            print(f"Normalizing {field} for {source}...")
            df[field] = df[field].apply(normalize_phone)
        elif field in ["first_name", "last_name", "email"]:
            print(f"Normalizing {field} for {source}...")
            df[field] = df[field].apply(normalize_text)

    dedupe_fields = ["first_name", "last_name", "email", "phone_number", "address", "date_of_birth"]
    for field in dedupe_fields:
        if field in df.columns:
            df[field] = df[field].apply(lambda x: str(x) if pd.notnull(x) else None)

    return df

def preprocess_all_data():
    """Load cleaned data from Parquet, preprocess it, and save back to Parquet."""
    datasets = {}
    for source in ["clinic", "urgent_care", "hospital", "physical_therapy"]:
        input_path = PARQUET_DIR / f"{source}_cleaned.parquet"
        output_path = PARQUET_DIR / f"{source}_preprocessed.parquet"

        if input_path.exists():
            print(f"Loading {source} data from {input_path}...")
            df = pd.read_parquet(input_path)

            df = preprocess_source_data(df, source)

            if "id" not in df.columns:
                raise ValueError(f"'id' column is missing for {source} data")

            print(f"Saving preprocessed {source} data to {output_path}...")
            df.to_parquet(output_path, index=False)
            datasets[source] = df
        else:
            print(f"Warning: {input_path} does not exist!")

    return datasets

if __name__ == "__main__":
    preprocess_all_data()
