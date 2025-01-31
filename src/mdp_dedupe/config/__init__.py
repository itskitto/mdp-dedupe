"""Configuration management for the mdp-dedupe package."""
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml
from dotenv import load_dotenv
from typing_extensions import TypedDict

# Load environment variables from .env file
load_dotenv()

class PathConfig(TypedDict):
    """Type definition for path configuration."""
    processed_data: str
    models: str
    results: str

class DatabaseConfig(TypedDict):
    """Type definition for database configuration."""
    url: str
    echo: bool

class AppConfig(TypedDict):
    """Type definition for application configuration."""
    paths: PathConfig
    database: DatabaseConfig

def _resolve_env_vars(value: str) -> str:
    """Resolve environment variables in a string.

    Args:
        value: String potentially containing environment variables.

    Returns:
        String with environment variables resolved.
    """
    if not isinstance(value, str):
        return value

    # Replace ${VAR} or $VAR with environment variable values
    if "${" in value:
        for env_var in os.environ:
            value = value.replace(f"${{{env_var}}}", os.environ[env_var])
    return value

def _process_config_values(config: Dict[str, Any]) -> Dict[str, Any]:
    """Process configuration values, resolving environment variables.

    Args:
        config: Raw configuration dictionary.

    Returns:
        Processed configuration dictionary.
    """
    processed = {}
    for key, value in config.items():
        if isinstance(value, dict):
            processed[key] = _process_config_values(value)
        elif isinstance(value, str):
            processed[key] = _resolve_env_vars(value)
        else:
            processed[key] = value
    return processed

class Config:
    """Configuration manager for the application.

    This class handles loading and accessing configuration values from
    YAML files, with support for environment variables.
    """

    def __init__(
        self,
        config_path: Optional[Union[str, Path]] = None,
        env: str = "default"
    ) -> None:
        """Initialize the configuration manager.

        Args:
            config_path: Path to the configuration file. If not provided,
                defaults to the package's default configuration.
            env: Environment name to load specific configurations.
                Defaults to "default".

        Raises:
            FileNotFoundError: If the configuration file doesn't exist.
            ValueError: If the configuration file is invalid.
        """
        self.env = env
        self._config: AppConfig = self._load_config(config_path)

    def _load_config(
        self,
        config_path: Optional[Union[str, Path]] = None
    ) -> AppConfig:
        """Load configuration from the specified file.

        Args:
            config_path: Path to the configuration file.
                If not provided, uses the default package configuration.

        Returns:
            AppConfig: Loaded configuration dictionary.

        Raises:
            FileNotFoundError: If the configuration file doesn't exist.
            ValueError: If the configuration is invalid.
        """
        if config_path is None:
            config_path = Path(__file__).parent / "default.yaml"
        else:
            config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}"
            )

        with open(config_path, "r") as file:
            try:
                config = yaml.safe_load(file)
                # Process environment variables
                config = _process_config_values(config)

                # Validate required sections
                if not isinstance(config, dict):
                    raise ValueError("Configuration must be a dictionary")
                if "paths" not in config:
                    raise ValueError("Configuration must contain 'paths' section")
                if "database" not in config:
                    raise ValueError("Configuration must contain 'database' section")

                # Set database echo from environment if specified
                if os.getenv("DB_ECHO"):
                    config["database"]["echo"] = os.getenv("DB_ECHO").lower() == "true"

                return config
            except yaml.YAMLError as e:
                raise ValueError(f"Invalid YAML configuration: {str(e)}")

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value using dot notation.

        Args:
            key: Configuration key in dot notation (e.g., "paths.models")
            default: Default value if the key doesn't exist

        Returns:
            The configuration value or the default if not found
        """
        keys = key.split(".")
        value = self._config
        for k in keys:
            if not isinstance(value, dict):
                return default
            value = value.get(k, {})
            if not value:
                return default
        return value or default

    @property
    def paths(self) -> PathConfig:
        """Get the paths configuration section.

        Returns:
            PathConfig: Dictionary containing path configurations
        """
        return self._config["paths"]

    @property
    def database(self) -> DatabaseConfig:
        """Get the database configuration section.

        Returns:
            DatabaseConfig: Dictionary containing database configurations
        """
        return self._config["database"]
