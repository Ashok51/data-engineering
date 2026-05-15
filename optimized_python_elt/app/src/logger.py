import logging
import sys

def setup_logger() -> logging.logger:
    logger = Logging.getLogger("de_pipeline_logger")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
