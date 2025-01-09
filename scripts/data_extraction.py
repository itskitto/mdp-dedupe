import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from models.clinic import ClinicPatient
from models.urgent_care import UrgentCarePatient
from models.hospital import HospitalPatient
from models.physical_therapy import PhysicalTherapyPatient
import os

from scripts.common import load_config

config = load_config()

DATABASE_URL = config.get("database.url")
engine = create_engine(DATABASE_URL)

PROCESSED_PARQUET_DIR = os.path.join(config.get("paths.processed_data"), "parquet")
os.makedirs(PROCESSED_PARQUET_DIR, exist_ok=True)

def fetch_table_data(session: Session, model):
    """
    Fetch data from a table and return it as a Pandas DataFrame.
    :param session: SQLAlchemy session object.
    :param model: SQLAlchemy model representing the table.
    :return: Pandas DataFrame of table data.
    """
    query = session.query(model)
    return pd.read_sql(query.statement, session.bind)

def preprocess_for_dedupe(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """Dynamically map and preprocess data for deduplication."""
    field_mappings = config.get(f"field_mappings.{source_name}", {})
    for source_field, target_field in field_mappings.items():
        if source_field in df.columns:
            df[target_field] = df[source_field]
    return df

def extract_all_data():
    """
    Extract data from all source tables into Pandas DataFrames, applying dynamic preprocessing.
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
            
            output_path = os.path.join(PROCESSED_PARQUET_DIR, f"{source}_cleaned.parquet")
            processed_data.to_parquet(output_path, index=False)
            print(f"Saved {source} data to {output_path}")

if __name__ == "__main__":
    extract_all_data()
