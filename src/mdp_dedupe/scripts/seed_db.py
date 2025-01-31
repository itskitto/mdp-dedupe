"""Database seeding script for generating test data."""
from sqlalchemy.orm import Session
from faker import Faker
from typing import List, Dict

from mdp_dedupe.models.clinic import ClinicPatient
from mdp_dedupe.models.urgent_care import UrgentCarePatient
from mdp_dedupe.models.hospital import HospitalPatient
from mdp_dedupe.models.physical_therapy import PhysicalTherapyPatient
from mdp_dedupe.models.base import Base
from mdp_dedupe.config import Config

# Initialize configuration
config = Config()
DATABASE_URL = config.database["url"]

from sqlalchemy import create_engine
engine = create_engine(DATABASE_URL)
SessionLocal = Session(bind=engine)

fake = Faker()
MAX_PHONE_LENGTH = 15

def generate_shared_pool(size: int) -> List[Dict]:
    """Generate a shared pool of patient records to ensure duplicates.

    Args:
        size: Number of patient records to generate.

    Returns:
        List of dictionaries containing patient data.
    """
    pool = []
    for _ in range(size):
        patient = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "date_of_birth": fake.date_of_birth(),
            "phone_number": fake.phone_number()[:MAX_PHONE_LENGTH],
            "email": fake.email(),
            "address": fake.address(),
            "insurance_id": fake.bothify(text="INS###??"),
            "emergency_contact_name": fake.name(),
            "emergency_contact_phone": fake.phone_number()[:MAX_PHONE_LENGTH],
        }
        pool.append(patient)
    return pool

def seed_duplicates(session: Session, model, pool: List[Dict], num_duplicates: int, randomize_fields=None):
    """Seed duplicate records into a table.

    Args:
        session: SQLAlchemy session.
        model: SQLAlchemy model to seed.
        pool: Shared pool of patient data.
        num_duplicates: Number of duplicates to insert.
        randomize_fields: Fields to slightly vary for duplicates.
    """
    for i in range(num_duplicates):
        patient = pool[i % len(pool)].copy()  # Copy to avoid modifying the original pool
        if randomize_fields:
            for field in randomize_fields:
                if field == "email":
                    patient["email"] = fake.email()
                elif field == "address":
                    patient["address"] = fake.address()

        session.add(model(**adjust_fields_for_model(patient, model)))
    session.commit()

def seed_unique_records(session: Session, model, num_unique: int):
    """Seed unique records into a table.

    Args:
        session: SQLAlchemy session.
        model: SQLAlchemy model to seed.
        num_unique: Number of unique records to insert.
    """
    for _ in range(num_unique):
        unique_patient = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "date_of_birth": fake.date_of_birth(),
            "phone_number": fake.phone_number()[:MAX_PHONE_LENGTH],
            "email": fake.email(),
            "address": fake.address(),
            "insurance_id": fake.bothify(text="INS###??"),
            "emergency_contact_name": fake.name(),
            "emergency_contact_phone": fake.phone_number()[:MAX_PHONE_LENGTH],
        }
        session.add(model(**adjust_fields_for_model(unique_patient, model)))
    session.commit()

def adjust_fields_for_model(data: Dict, model):
    """Adjust shared data to match the fields expected by the specific model.

    Args:
        data: Shared data dictionary.
        model: SQLAlchemy model.

    Returns:
        Adjusted dictionary matching the model's fields.
    """
    if model == ClinicPatient:
        return {
            "first_name": data["first_name"],
            "last_name": data["last_name"],
            "date_of_birth": data["date_of_birth"],
            "phone_number": data["phone_number"],
            "email": data["email"],
            "address": data["address"],
            "insurance_id": data["insurance_id"],
        }
    elif model == UrgentCarePatient:
        return {
            "first_name": data["first_name"],
            "last_name": data["last_name"],
            "dob": data["date_of_birth"],
            "phone": data["phone_number"],
            "email": data["email"],
            "address_line": data["address"],
            "insurance_id": data["insurance_id"],
            "emergency_contact_name": data["emergency_contact_name"],
            "emergency_contact_phone": data["emergency_contact_phone"],
        }
    elif model == HospitalPatient:
        return {
            "first_name": data["first_name"],
            "middle_name": fake.first_name(),
            "last_name": data["last_name"],
            "date_of_birth": data["date_of_birth"],
            "phone_number": data["phone_number"],
            "email_address": data["email"],
            "address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "zip": fake.zipcode(),
            },
            "insurance_provider": fake.company(),
            "policy_number": fake.bothify(text="POLICY###??"),
        }
    elif model == PhysicalTherapyPatient:
        return {
            "full_name": f"{data['first_name']} {data['last_name']}",
            "dob": data["date_of_birth"],
            "contact_phone": data["phone_number"],
            "email": data["email"],
            "street_address": data["address"],
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "insurance": fake.company(),
        }

def seed_table(session: Session, model, pool: List[Dict], num_duplicates: int, num_unique: int, randomize_fields=None):
    """Seed a table with both duplicate and unique records.

    Args:
        session: SQLAlchemy session.
        model: SQLAlchemy model to seed.
        pool: Shared pool of patient data.
        num_duplicates: Number of duplicate records to insert.
        num_unique: Number of unique records to insert.
        randomize_fields: Fields to slightly vary for duplicates.
    """
    seed_duplicates(session, model, pool, num_duplicates, randomize_fields)
    seed_unique_records(session, model, num_unique)

def seed_all_tables(shared_pool: List[Dict], duplicates_per_table: int, unique_per_table: int):
    """Seed all tables with controlled duplicates and unique records.

    Args:
        shared_pool: Shared pool of patient data.
        duplicates_per_table: Number of duplicates to insert in each table.
        unique_per_table: Number of unique records to insert in each table.
    """
    tables = {
        ClinicPatient: ["email"],
        UrgentCarePatient: ["address"],
        HospitalPatient: [],
        PhysicalTherapyPatient: ["email", "address"],
    }

    session = SessionLocal

    try:
        for model, randomize_fields in tables.items():
            print(f"Seeding table: {model.__tablename__} with {duplicates_per_table} duplicates and {unique_per_table} unique records...")
            seed_table(session, model, shared_pool, duplicates_per_table, unique_per_table, randomize_fields)
    finally:
        session.close()

if __name__ == "__main__":
    # Create all tables
    Base.metadata.create_all(engine)

    # Generate a shared pool of 10 patient records
    shared_patient_pool = generate_shared_pool(10)

    # Define how many duplicates and unique records to create per table
    duplicates_per_table = 5
    unique_per_table = 5

    # Seed all tables
    seed_all_tables(shared_patient_pool, duplicates_per_table, unique_per_table)
