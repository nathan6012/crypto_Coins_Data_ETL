from pathlib import Path
import logging


def setup_logger(name: str = "etl_pipeline", log_dir: str = "local_data"):
    """
    Creates a reusable ETL logger with file + console output.
    """

    # -----------------------------
    # Create log directory
    # -----------------------------
    root_dir = Path(__file__).resolve().parent.parent
    log_path = root_dir / log_dir
    log_path.mkdir(parents=True, exist_ok=True)

    log_file = log_path / f"{name}.log"

    # -----------------------------
    # Logger setup
    # -----------------------------
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # avoid duplicate logs

    # Prevent duplicate handlers
    if not logger.handlers:

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger, str(log_file)