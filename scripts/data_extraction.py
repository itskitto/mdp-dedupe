import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from models.clinic import ClinicPatient
from models.urgent_care import UrgentCarePatient
from models.hospital import HospitalPatient
from models.physical_therapy import PhysicalTherapyPatient
from config import DATABASE_URL

# SQLAlchemy engine
engine = create_engine(DATABASE_URL)

def fetch_table_data(session: Session, model):
    """
    Fetch data from a table and return it as a Pandas DataFrame.
    :param session: SQLAlchemy session object.
    :param model: SQLAlchemy model representing the table.
    :return: Pandas DataFrame of table data.
    """
    query = session.query(model)
    return pd.read_sql(query.statement, session.bind)

def extract_all_data():
    """
    Extract data from all source tables into Pandas DataFrames.
    :return: Dictionary of DataFrames.
    """
    dataframes = {}
    with Session(engine) as session:
        # Extract data from each table
        dataframes["clinic"] = fetch_table_data(session, ClinicPatient)
        dataframes["urgent_care"] = fetch_table_data(session, UrgentCarePatient)
        dataframes["hospital"] = fetch_table_data(session, HospitalPatient)
        dataframes["physical_therapy"] = fetch_table_data(session, PhysicalTherapyPatient)

    return dataframes

if __name__ == "__main__":
    # Extract data
    data = extract_all_data()
    
    # Print summary of each DataFrame
    for source, df in data.items():
        print(f"Source: {source}")
        print(df.info())
        print(df.head())
