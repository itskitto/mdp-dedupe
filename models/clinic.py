from sqlalchemy import Column, Integer, String, Date, Text
from .base import Base

class ClinicPatient(Base):
    __tablename__ = "clinic_patients"
    patient_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(255))
    last_name = Column(String(255))
    date_of_birth = Column(Date)
    phone_number = Column(String(15))
    email = Column(String(255))
    address = Column(Text)
    insurance_id = Column(String(50))
