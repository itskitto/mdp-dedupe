"""Core deduplication logic for matching and clustering similar records."""
import logging
import multiprocessing
import os
from pathlib import Path
from typing import Dict, List, Optional

import dedupe
import pandas as pd

from mdp_dedupe.config import Config
from mdp_dedupe.core.types import (
    ClusterID,
    ClusterResult,
    DedupeResults,
    RecordDict,
    RecordID,
)

logger = logging.getLogger(__name__)

class DeduplicationError(Exception):
    """Base exception for deduplication-related errors."""
    pass

class ModelNotFoundError(DeduplicationError):
    """Raised when a required model file is not found."""
    pass

class InvalidDataError(DeduplicationError):
    """Raised when input data is invalid or missing required fields."""
    pass

def prepare_dedupe_data(dataframes: Dict[str, pd.DataFrame]) -> Dict[RecordID, RecordDict]:
    """Combine all records into a format suitable for dedupe.

    Args:
        dataframes: Dictionary mapping source names to their respective DataFrames.

    Returns:
        Dictionary mapping record IDs to their data dictionaries.

    Raises:
        InvalidDataError: If any required fields are missing or in wrong format.
    """
    if not dataframes:
        raise InvalidDataError("No data provided for deduplication")

    try:
        combined_data: Dict[RecordID, RecordDict] = {}
        for source, df in dataframes.items():
            required_columns = {"id", "first_name", "last_name", "date_of_birth"}
            missing_columns = required_columns - set(df.columns)
            if missing_columns:
                raise InvalidDataError(
                    f"Missing required columns in {source}: {missing_columns}"
                )

            for idx, row in df.iterrows():
                record_id = f"{source}_{row['id']}"
                combined_data[record_id] = row.to_dict()

        if not combined_data:
            raise InvalidDataError("No records found in provided data")

        return combined_data
    except Exception as e:
        raise InvalidDataError(f"Error preparing data: {str(e)}")

def setup_dedupe(
    config: Config,
    training_data: Dict[RecordID, RecordDict]
) -> dedupe.StaticDedupe:
    """Set up and train the dedupe model.

    Args:
        config: Application configuration instance.
        training_data: Dictionary of records for training.

    Returns:
        Trained dedupe model instance.

    Raises:
        ModelNotFoundError: If model settings file not found when expected.
        DeduplicationError: For other deduplication-related errors.
    """
    settings_file = Path(config.paths["models"]) / "dedupe_model_settings"

    try:
        if settings_file.exists():
            logger.info(f"Loading existing model from {settings_file}")
            with open(settings_file, "rb") as f:
                return dedupe.StaticDedupe(f)

        logger.info("Training new dedupe model...")
        multiprocessing.set_start_method('spawn', force=True)

        from dedupe.variables import String
        fields = [
            String('first_name', has_missing=True),
            String('last_name', has_missing=True),
            String('email', has_missing=True),
            String('phone_number', has_missing=True),
            String('address', has_missing=True),
            String('date_of_birth', has_missing=True)
        ]

        deduper = dedupe.Dedupe(fields)
        deduper.prepare_training(training_data)

        logger.info("Starting active labeling...")
        dedupe.console_label(deduper)

        deduper.train()

        # Save trained model
        logger.info(f"Saving trained model to {settings_file}")
        os.makedirs(settings_file.parent, exist_ok=True)
        with open(settings_file, "wb") as f:
            deduper.write_settings(f)

        return deduper

    except FileNotFoundError as e:
        raise ModelNotFoundError(f"Model file not found: {str(e)}")
    except Exception as e:
        raise DeduplicationError(f"Error setting up dedupe: {str(e)}")

def deduplicate_records(
    config: Config,
    data: Optional[Dict[str, pd.DataFrame]] = None
) -> Path:
    """Run the deduplication process on the provided data.

    Args:
        config: Application configuration instance.
        data: Optional dictionary of DataFrames to process.
            If not provided, loads from default locations.

    Returns:
        Path to the results file.

    Raises:
        InvalidDataError: If input data is invalid.
        DeduplicationError: For other deduplication-related errors.
    """
    try:
        if data is None:
            data = {}
            parquet_dir = Path(config.paths["processed_data"]) / "parquet"
            for source in ["clinic", "urgent_care", "hospital", "physical_therapy"]:
                path = parquet_dir / f"{source}_preprocessed.parquet"
                if path.exists():
                    logger.info(f"Loading {source} data from {path}")
                    data[source] = pd.read_parquet(path)
                else:
                    logger.warning(f"Data file not found: {path}")

        if not data:
            raise InvalidDataError(
                "No data files found. Please ensure data files exist in the "
                "processed_data/parquet directory."
            )

        dedupe_data = prepare_dedupe_data(data)
        deduper = setup_dedupe(config, dedupe_data)

        # Set clustering parameters
        threshold = config.get("dedupe.threshold", 0.5)
        logger.info(f"Clustering records with threshold {threshold}...")

        # Run clustering
        clusters: DedupeResults = deduper.partition(
            dedupe_data,
            threshold=threshold
        )

        # Save results
        results_dir = Path(config.paths["results"])
        os.makedirs(results_dir, exist_ok=True)
        output_path = results_dir / "dedupe_results.csv"

        logger.info(f"Saving results to {output_path}")
        with open(output_path, "w") as f:
            f.write("cluster_id, record_id, confidence_score\n")
            for cluster_id, cluster_data in enumerate(clusters):
                records, scores = cluster_data
                for record_id in records:
                    # Take the mean of the scores for a single confidence value
                    confidence = float(sum(scores)) / len(scores)
                    f.write(f"{cluster_id}, {record_id}, {confidence:.4f}\n")

        return output_path

    except (InvalidDataError, ModelNotFoundError) as e:
        raise
    except Exception as e:
        raise DeduplicationError(f"Deduplication failed: {str(e)}")

if __name__ == "__main__":
    multiprocessing.freeze_support()
