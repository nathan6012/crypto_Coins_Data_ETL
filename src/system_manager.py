import sys
import os 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pathlib import Path
import logging 


def setup_logger(name: str = "etl_pipeline", log_dir: str = "data"):
  """Creates a logger that writes logs into /data folder."""

    # Ensure /data folder exists
  log_path = Path(log_dir)
  log_path.mkdir(parents=True, exist_ok=True)

    # Log file inside data/
  log_file = log_path / f"{name}.log"

  logger = logging.getLogger(name)
  logger.setLevel(logging.INFO)

    # Avoid duplicate handlers (important in ETL runs / Prefect)
  if not logger.handlers:
        # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

        # Format
    formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
    file_handler.setFormatter(formatter)

        # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

        # Attach handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

  return logger, str(log_file)