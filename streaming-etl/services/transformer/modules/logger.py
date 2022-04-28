import logging
import logging.handlers
import os
import sys

LOGS_PATH = "logs"
LOG_FILENAME = "log"
LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(filename)s -> %(funcName)s :: %(message)s"
)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
FORMATTER = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)


def get_stream_handler() -> logging.Handler:
    stream_handler = logging.StreamHandler(sys.stdout)

    stream_handler.setFormatter(FORMATTER)
    stream_handler.setLevel(logging.INFO)

    return stream_handler


def get_file_handler() -> logging.Handler:
    os.makedirs(LOGS_PATH, exist_ok=True)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        os.path.join(LOGS_PATH, LOG_FILENAME), when="d", backupCount=30, encoding="utf8"
    )

    file_handler.setFormatter(FORMATTER)
    file_handler.setLevel(logging.DEBUG)

    return file_handler


def get_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)

    logger.setLevel(logging.INFO)
    logger.addHandler(get_stream_handler())
    logger.addHandler(get_file_handler())

    return logger


LOGGER = get_logger()
