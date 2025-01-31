"""SQLAlchemy model for hospital patient records."""
from sqlalchemy import Column, Integer, String, Date, JSON

from mdp_dedupe.models.base import Base, ModelMixin

class HospitalPatient(Base, ModelMixin):
    """Model representing a patient record from a hospital."""

    __tablename__ = "hospital_patients"

    patient_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(255))
    middle_name = Column(String(255))
    last_name = Column(String(255))
    date_of_birth = Column(Date)
    phone_number = Column(String(15))
    email_address = Column(String(255))
    address = Column(JSON)  # Stores structured address data
    insurance_provider = Column(String(255))
    policy_number = Column(String(50))
