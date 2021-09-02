import time

import pytest

from aqueduct.worker import batches


def long_elements_producer():
    """Long non-blocking producer of elements."""
    tic_elements = {1: 1, 4: 2, 6: 3, 9: 4}

    for tic in range(10):
        if tic in tic_elements:
            yield tic_elements[tic]
        else:
            yield
        time.sleep(0.01)


@pytest.mark.parametrize('elements, batch_size, result', [
    ([1, 2, 3], 1, [[1], [2], [3]]),
    ([1, 2, 3], 2, [[1, 2], [3]]),
    ([1, 2, 3], 4, [[1, 2, 3]]),
])
def test_batches_different_batch_sizes(elements, batch_size, result):
    assert list(batches(elements, batch_size, 1.)) == result


@pytest.mark.parametrize('timeout, result', [
    (0.01, [[1], [2], [3], [4]]),
    (0.03, [[1], [2, 3], [4]]),
])
def test_batches_different_timeouts(timeout, result):
    assert list(
        batches(long_elements_producer(), 4, timeout)
    ) == result
