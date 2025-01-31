"""Main entry point for the mdp-dedupe package."""
import argparse
import logging
import multiprocessing
import os
import sys
from pathlib import Path
from typing import Optional

from mdp_dedupe.config import Config
from mdp_dedupe.core.dedup_match import deduplicate_records
from mdp_dedupe.scripts.seed_db import (
    Base,
    engine,
    generate_shared_pool,
    seed_all_tables,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def ensure_directories(config: Config) -> None:
    """Create necessary data directories if they don't exist."""
    for path_key in ["processed_data", "models", "results"]:
        path = Path(config.paths[path_key])
        if path_key == "processed_data":
            # Also create parquet subdirectory
            path = path / "parquet"
        os.makedirs(path, exist_ok=True)
        logger.info(f"Ensured directory exists: {path}")

def run_seed_db(pool_size: int, duplicates: int, unique: int) -> None:
    """Run database seeding process."""
    logger.info("Creating database tables...")
    Base.metadata.create_all(engine)

    logger.info(f"Generating shared pool of {pool_size} patient records...")
    shared_patient_pool = generate_shared_pool(pool_size)

    logger.info(f"Seeding tables with {duplicates} duplicates and {unique} unique records each...")
    seed_all_tables(shared_patient_pool, duplicates, unique)
    logger.info("Database seeding completed successfully")

def run_extraction() -> None:
    """Run data extraction process."""
    from mdp_dedupe.scripts.data_extraction import extract_all_data

    logger.info("Extracting data from database to parquet files...")
    extract_all_data()
    logger.info("Data extraction completed successfully")

def run_preprocessing() -> None:
    """Run data preprocessing."""
    from mdp_dedupe.scripts.data_preprocessing import preprocess_all_data

    logger.info("Preprocessing extracted data...")
    preprocess_all_data()
    logger.info("Data preprocessing completed successfully")

def run_deduplication(config: Config) -> Optional[Path]:
    """Run deduplication process."""
    logger.info("Starting deduplication process...")
    try:
        result_path = deduplicate_records(config)
        logger.info(f"Deduplication completed successfully. Results saved to {result_path}")
        return result_path
    except Exception as e:
        logger.error(f"Deduplication failed: {str(e)}")
        return None

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Medical Data Provider Deduplication Tool"
    )

    # Pipeline control arguments
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run complete pipeline (seed, extract, preprocess, dedupe)",
    )
    parser.add_argument(
        "--seed",
        action="store_true",
        help="Seed the database with test data",
    )
    parser.add_argument(
        "--extract",
        action="store_true",
        help="Extract data from database to parquet files",
    )
    parser.add_argument(
        "--preprocess",
        action="store_true",
        help="Preprocess the extracted data",
    )
    parser.add_argument(
        "--dedupe",
        action="store_true",
        help="Run deduplication on preprocessed data",
    )

    # Seeding configuration
    parser.add_argument(
        "--pool-size",
        type=int,
        default=10,
        help="Size of shared patient pool for seeding (default: 10)",
    )
    parser.add_argument(
        "--duplicates",
        type=int,
        default=5,
        help="Number of duplicate records per table (default: 5)",
    )
    parser.add_argument(
        "--unique",
        type=int,
        default=5,
        help="Number of unique records per table (default: 5)",
    )

    # Configuration
    parser.add_argument(
        "--config",
        type=str,
        help="Path to custom configuration file",
    )

    return parser.parse_args()

def main() -> int:
    """Execute the main process based on command line arguments.

    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    args = parse_args()

    try:
        # Load configuration
        config = Config(args.config) if args.config else Config()
        ensure_directories(config)

        # Determine which steps to run
        run_all = args.all
        steps = []

        if args.seed or run_all:
            steps.append(lambda: run_seed_db(args.pool_size, args.duplicates, args.unique))
        if args.extract or run_all:
            steps.append(run_extraction)
        if args.preprocess or run_all:
            steps.append(run_preprocessing)
        if args.dedupe or run_all:
            steps.append(lambda: run_deduplication(config))

        # If no steps specified, default to deduplication only
        if not steps:
            steps.append(lambda: run_deduplication(config))

        # Execute each step
        for step in steps:
            step()

        return 0

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return 1

if __name__ == "__main__":
    # This guard is required for multiprocessing in dedupe
    multiprocessing.freeze_support()
    sys.exit(main())
