import logging
import sys

LOGGER_NAME = 'aqueduct'

log = logging.getLogger(LOGGER_NAME)
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s [Flow] [pid:%(process)d] %(message)s')
ch = logging.StreamHandler(sys.stderr)
ch.setFormatter(formatter)
log.addHandler(ch)
