"""Common utilities for scripts."""
from mdp_dedupe.config import Config

def load_config() -> Config:
    """Load the application configuration.

    Returns:
        Config: Application configuration instance.
    """
    return Config()
