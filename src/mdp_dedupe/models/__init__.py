"""SQLAlchemy models for different patient record types."""
from mdp_dedupe.models.base import Base
from mdp_dedupe.models.clinic import ClinicPatient
from mdp_dedupe.models.urgent_care import UrgentCarePatient
from mdp_dedupe.models.hospital import HospitalPatient
from mdp_dedupe.models.physical_therapy import PhysicalTherapyPatient

__all__ = [
    'Base',
    'ClinicPatient',
    'UrgentCarePatient',
    'HospitalPatient',
    'PhysicalTherapyPatient',
]
