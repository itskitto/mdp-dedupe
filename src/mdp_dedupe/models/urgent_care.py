"""SQLAlchemy model for urgent care patient records."""
from sqlalchemy import Column, Integer, String, Date, Text

from mdp_dedupe.models.base import Base, ModelMixin

class UrgentCarePatient(Base, ModelMixin):
    """Model representing a patient record from an urgent care facility."""

    __tablename__ = "urgent_care_patients"

    patient_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(255))
    last_name = Column(String(255))
    dob = Column(Date)
    phone = Column(String(15))
    email = Column(String(255))
    address_line = Column(Text)
    insurance_id = Column(String(50))
    emergency_contact_name = Column(String(255))
    emergency_contact_phone = Column(String(15))
