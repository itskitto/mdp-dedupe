"""SQLAlchemy model for physical therapy patient records."""
from sqlalchemy import Column, Integer, String, Date

from mdp_dedupe.models.base import Base, ModelMixin

class PhysicalTherapyPatient(Base, ModelMixin):
    """Model representing a patient record from a physical therapy facility."""

    __tablename__ = "physical_therapy_patients"

    patient_id = Column(Integer, primary_key=True, autoincrement=True)
    full_name = Column(String(255))
    dob = Column(Date)
    contact_phone = Column(String(15))
    email = Column(String(255))
    street_address = Column(String(255))
    city = Column(String(100))
    state = Column(String(2))
    zip_code = Column(String(10))
    insurance = Column(String(255))
