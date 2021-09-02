"""Counts the execution time of each step of the service.

Steps: preprocessing of the image, processing with a model and postprocessing.

To start, see README.md.
"""
import os
import pathlib
import time
from typing import List

import logging
import numpy as np
import torch

from maas.maas.models import get_pipeline

IMAGES_DIR = pathlib.Path(__file__).parent / 'data'
pipeline = get_pipeline()


def _images(*im_paths: str):
    if not im_paths:
        im_paths = [os.path.join(IMAGES_DIR, name) for name in ('apple.jpg', 'cat.jpg', 'umbrella.jpg')]
    for im_path in im_paths:
        logging.info(f'Image: {im_path.split("/")[-1]}, size: {os.path.getsize(im_path)} bytes')
        with open(im_path, 'rb') as f:
            yield f.read()


def _get_times(func, *args, **kwargs) -> List[float]:
    """Returns time in seconds."""
    times = []
    for _ in range(20):
        t1 = time.time()
        func(*args, **kwargs)
        times.append((time.time() - t1))
    return times


def _mean_time(times: List[float], units: str = 'ms'):
    k = 1
    if units == 'ms':
        k = 10**3
    times = np.array(times) * k
    return f'{times.mean():.2f} +- {times.std():.2f} {units}'


def test_batch_processing():
    """Calculates just `process_list` method time.

    This time does not depend on image size because model uses image with fixed size.
    """
    logging.info('\nTest batch processing time\n')

    im = next(_images())
    im_torch = pipeline.preprocessor.process(im)

    def process(_im_torch, _batch_size):
        pipeline.classifier.process_list(data=[_im_torch] * _batch_size)
        torch.cuda.synchronize(0)

    for batch_size in [2 ** i for i in range(5)]:
        for _ in range(10):
            process(im_torch, batch_size)

        _times = [t / batch_size for t in _get_times(process, im_torch, batch_size)]
        logging.info(f'Batch size: {batch_size}\t Time: {_mean_time(_times)}')


def test_preprocessing():
    logging.info('\nTest preprocessing time\n')

    for im in _images():
        _times = _get_times(pipeline.preprocessor.process, im)
        logging.info(f'Time: {_mean_time(_times)}')


def test_postprocessing():
    logging.info('\nTest postprocessing time\n')

    im = next(_images())
    im_torch = pipeline.preprocessor.process(im)
    pred = pipeline.classifier.process_list(data=[im_torch])[0]

    _times = _get_times(pipeline.postprocessor.process, pred)
    logging.info(f'Time: {_mean_time(_times)}')


if __name__ == '__main__':
    test_preprocessing()
    test_batch_processing()
    test_postprocessing()
