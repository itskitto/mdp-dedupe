"""Base model configuration for SQLAlchemy models."""
from typing import Any, Dict

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import registry

# Create the registry
mapper_registry = registry()

# Create the base class using the registry
Base = mapper_registry.generate_base()

class ModelMixin:
    """Mixin class providing common functionality for all models."""

    def to_dict(self) -> Dict[str, Any]:
        """Convert model instance to dictionary.

        Returns:
            Dict[str, Any]: Dictionary representation of the model instance.
        """
        return {
            column.name: getattr(self, column.name)
            for column in self.__table__.columns
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelMixin':
        """Create model instance from dictionary.

        Args:
            data: Dictionary containing model data.

        Returns:
            ModelMixin: New instance of the model.
        """
        return cls(**{
            k: v for k, v in data.items()
            if k in cls.__table__.columns.keys()
        })
