from sqlalchemy import Column, Integer, String, Date
from .base import Base

class PhysicalTherapyPatient(Base):
    __tablename__ = "physical_therapy_patients"
    pt_patient_id = Column(Integer, primary_key=True, autoincrement=True)
    full_name = Column(String(255))
    dob = Column(Date)
    contact_phone = Column(String(15))
    email = Column(String(255))
    street_address = Column(String(255))
    city = Column(String(100))
    state = Column(String(50))
    zip_code = Column(String(10))
    insurance = Column(String(255))