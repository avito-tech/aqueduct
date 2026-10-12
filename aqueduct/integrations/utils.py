import os
from typing import Optional


FLOW_START_TIMEOUT_NAME = 'FLOW_START_TIMEOUT'
FLOW_START_TIMEOUT_DEFAULT = 300.0


def get_start_timeout() -> Optional[float]:
    try:
        start_timeout = float(os.environ.get(
            FLOW_START_TIMEOUT_NAME,
            FLOW_START_TIMEOUT_DEFAULT,
        ))
    except ValueError:
        start_timeout = FLOW_START_TIMEOUT_DEFAULT
    if start_timeout <= 0:
        return None
    return start_timeout
