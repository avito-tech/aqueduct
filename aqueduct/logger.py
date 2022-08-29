import logging
import sys

LOGGER_NAME = 'aqueduct'

log = logging.getLogger(LOGGER_NAME)
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s [Flow] [pid:%(process)d] %(message)s')
ch = logging.StreamHandler(sys.stderr)
ch.setFormatter(formatter)
log.addHandler(ch)


def replace_logger(custom_logger: logging.Logger) -> None:
    """Replaces default aqueduct logger with custom one"""
    log.name = custom_logger.name
    log.level = custom_logger.level
    log.parent = custom_logger.parent
    log.propagate = custom_logger.propagate
    log.handlers = custom_logger.handlers
    log.disabled = custom_logger.disabled
