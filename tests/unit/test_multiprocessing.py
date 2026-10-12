import logging
import warnings

import pytest

from aqueduct.logger import LOGGER_NAME
from aqueduct.multiprocessing import start_processes


def _noop(i):
    pass


def _start_spawned(**kwargs):
    context = start_processes(_noop, nprocs=2, join=False, daemon=True, start_method='spawn', **kwargs)
    assert context.join_all(timeout=30)
    return context


def test_on_start_wait_is_deprecated():
    with pytest.warns(DeprecationWarning, match='on_start_wait'):
        _start_spawned(on_start_wait=0.1)


def test_no_warning_when_on_start_wait_is_not_set(caplog):
    # the race that on_start_wait used to paper over is fixed (#66), so starting several
    # spawned subprocesses without it must neither log nor warn about anything
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME), warnings.catch_warnings():
        warnings.simplefilter('error')
        _start_spawned()
    assert 'on_start_wait' not in caplog.text
