import logging
import sys

LOGGER_NAME = 'aqueduct'

log = logging.getLogger(LOGGER_NAME)
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s [Flow] [pid:%(process)d] %(message)s')
ch = logging.StreamHandler(sys.stderr)
ch.setFormatter(formatter)
log.addHandler(ch)


def replace_logger(logger: logging.Logger) -> None:
    """Replaces default aqueduct logger with custom one"""
    log.name = logger.name
    log.level = logger.level
    log.parent = logger.parent
    log.propagate = logger.propagate
    log.handlers = logger.handlers
    log.disabled = logger.disabled
