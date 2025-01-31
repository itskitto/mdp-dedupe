"""Script to run deduplication using existing model."""
import logging
from pathlib import Path
import pandas as pd

from mdp_dedupe.config import Config
from mdp_dedupe.core.dedup_match import deduplicate_records

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_preprocessed_data():
    """Load preprocessed data from all sources."""
    data = {}
    parquet_dir = Path("data/processed/parquet")

    for source in ["clinic", "urgent_care", "hospital", "physical_therapy"]:
        path = parquet_dir / f"{source}_preprocessed.parquet"
        if path.exists():
            logger.info(f"Loading {source} data from {path}")
            df = pd.read_parquet(path)
            # Add id column if not present
            if 'id' not in df.columns:
                df['id'] = df.index + 1
            data[source] = df
        else:
            logger.warning(f"Data file not found: {path}")

    return data

def main():
    """Main function."""
    config = Config()

    # Load preprocessed data
    logger.info("Loading preprocessed data...")
    data = load_preprocessed_data()

    # Run deduplication
    logger.info("Running deduplication...")
    results_path = deduplicate_records(config, data)
    logger.info(f"Results saved to: {results_path}")

if __name__ == "__main__":
    main()
