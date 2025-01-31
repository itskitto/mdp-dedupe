"""Data extraction script for pulling data from database to parquet files."""
import os
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

from mdp_dedupe.models.clinic import ClinicPatient
from mdp_dedupe.models.urgent_care import UrgentCarePatient
from mdp_dedupe.models.hospital import HospitalPatient
from mdp_dedupe.models.physical_therapy import PhysicalTherapyPatient
from mdp_dedupe.scripts.common import load_config

config = load_config()
DATABASE_URL = config.database["url"]
engine = create_engine(DATABASE_URL)

PROCESSED_PARQUET_DIR = Path(config.paths["processed_data"]) / "parquet"
os.makedirs(PROCESSED_PARQUET_DIR, exist_ok=True)

def fetch_table_data(session: Session, model):
    """Fetch data from a table and return it as a Pandas DataFrame.

    Args:
        session: SQLAlchemy session object.
        model: SQLAlchemy model representing the table.

    Returns:
        Pandas DataFrame of table data.
    """
    query = session.query(model)
    return pd.read_sql(query.statement, session.bind)

def preprocess_for_dedupe(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """Dynamically map and preprocess data for deduplication.

    Args:
        df: Input DataFrame.
        source_name: Name of the data source.

    Returns:
        Preprocessed DataFrame with standardized field names.
    """
    # Create a new DataFrame with standardized fields
    mapped_data = pd.DataFrame()

    # Copy the ID field
    mapped_data['patient_id'] = df['patient_id']

    # Define standard field mappings for each source
    if source_name == 'clinic':
        # Clinic already uses standard names, copy directly
        standard_fields = ['first_name', 'last_name', 'date_of_birth', 'phone_number', 'email', 'address']
        for field in standard_fields:
            if field in df.columns:
                mapped_data[field] = df[field]

    elif source_name == 'urgent_care':
        # Map urgent care fields to standard names
        mapped_data['first_name'] = df['first_name']
        mapped_data['last_name'] = df['last_name']
        mapped_data['date_of_birth'] = df['dob']
        mapped_data['phone_number'] = df['phone']
        mapped_data['email'] = df['email']
        mapped_data['address'] = df['address_line']

    elif source_name == 'hospital':
        # Map hospital fields to standard names
        mapped_data['first_name'] = df['first_name']
        mapped_data['last_name'] = df['last_name']
        mapped_data['date_of_birth'] = df['date_of_birth']
        mapped_data['phone_number'] = df['phone_number']
        mapped_data['email'] = df['email_address']
        # Handle JSON address
        mapped_data['address'] = df['address'].apply(
            lambda x: f"{x['street']}, {x['city']}, {x['state']} {x['zip']}" if isinstance(x, dict) else x
        )

    elif source_name == 'physical_therapy':
        # Handle full name splitting
        if 'full_name' in df.columns:
            name_parts = df['full_name'].str.split(' ', n=1)
            mapped_data['first_name'] = name_parts.str[0]
            mapped_data['last_name'] = name_parts.str[1]

        # Map other fields
        mapped_data['date_of_birth'] = df['dob']
        mapped_data['phone_number'] = df['contact_phone']
        mapped_data['email'] = df['email']

        # Combine address fields
        address_parts = []
        if 'street_address' in df.columns:
            address_parts.append(df['street_address'])
        if all(field in df.columns for field in ['city', 'state', 'zip_code']):
            address_parts.extend([df['city'], df['state'], df['zip_code']])
        if address_parts:
            mapped_data['address'] = pd.Series([', '.join(str(p) for p in parts if pd.notna(p))
                                              for parts in zip(*address_parts)])

    return mapped_data

def extract_all_data():
    """Extract data from all source tables into Pandas DataFrames, applying dynamic preprocessing.
    Save each processed DataFrame as a Parquet file.
    """
    with Session(engine) as session:
        for source, model in {
            "clinic": ClinicPatient,
            "urgent_care": UrgentCarePatient,
            "hospital": HospitalPatient,
            "physical_therapy": PhysicalTherapyPatient,
        }.items():
            print(f"Processing {source} data...")

            raw_data = fetch_table_data(session, model)
            processed_data = preprocess_for_dedupe(raw_data, source)

            output_path = PROCESSED_PARQUET_DIR / f"{source}_cleaned.parquet"
            processed_data.to_parquet(output_path, index=False)
            print(f"Saved {source} data to {output_path}")

if __name__ == "__main__":
    extract_all_data()
