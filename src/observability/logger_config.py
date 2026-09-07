import logging
import sys
from pythonjsonlogger import jsonlogger


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger that outputs structured JSON instead of plain text.

    JSON output means every log line can be parsed reliably later
    (by a script, or loaded into Snowflake) instead of relying on
    fragile text-matching against a sentence.

    Usage:
        from src.observability.logger_config import get_logger
        logger = get_logger(__name__)
        logger.info("Extraction started", extra={"run_id": run_id, "stage": "extract"})
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        # Avoid adding duplicate handlers if this is called more than once
        return logger

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level"},
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger