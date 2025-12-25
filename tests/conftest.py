import sys

import pytest


pytest_plugins = 'aiohttp.pytest_plugin'

only_py310 = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="requires Python 3.10+",
)
