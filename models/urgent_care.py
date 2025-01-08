from sqlalchemy import Column, Integer, String, Date
from .base import Base

class UrgentCarePatient(Base):
    __tablename__ = "urgent_care_patients"
    patient_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(255))
    last_name = Column(String(255))
    dob = Column(Date)  # Different name for date_of_birth
    phone = Column(String(15))
    email = Column(String(255))
    address_line = Column(String(255))
    insurance_id = Column(String(50))
    emergency_contact_name = Column(String(255))
    emergency_contact_phone = Column(String(15))