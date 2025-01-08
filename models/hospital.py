from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.dialects.postgresql import JSON
from .base import Base

class HospitalPatient(Base):
    __tablename__ = "hospital_patients"
    hospital_patient_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(255))
    middle_name = Column(String(255))
    last_name = Column(String(255))
    date_of_birth = Column(Date)
    phone_number = Column(String(15))
    email_address = Column(String(255))
    address = Column(JSON)  # Use JSON type for the address
    insurance_provider = Column(String(255))
    policy_number = Column(String(50))
