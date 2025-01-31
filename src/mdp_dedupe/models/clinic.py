"""SQLAlchemy model for clinic patient records."""
from datetime import date
from typing import Optional

from sqlalchemy import Column, Integer, String, Date, Text

from mdp_dedupe.models.base import Base, ModelMixin

class ClinicPatient(Base, ModelMixin):
    """Model representing a patient record from a clinic.

    This model stores patient information from clinic sources, including
    personal details and contact information.
    """

    __tablename__ = "clinic_patients"

    patient_id: int = Column(Integer, primary_key=True, autoincrement=True)
    first_name: Optional[str] = Column(String(255))
    last_name: Optional[str] = Column(String(255))
    date_of_birth: Optional[date] = Column(Date)
    phone_number: Optional[str] = Column(String(15))
    email: Optional[str] = Column(String(255))
    address: Optional[str] = Column(Text)
    insurance_id: Optional[str] = Column(String(50))

    def __repr__(self) -> str:
        """Return string representation of the patient record.

        Returns:
            str: String representation including key identifying information.
        """
        return (
            f"ClinicPatient(patient_id={self.patient_id}, "
            f"name='{self.first_name} {self.last_name}', "
            f"dob={self.date_of_birth})"
        )
