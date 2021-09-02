import asyncio
import pathlib
from typing import Union

from bench.base import IMAGES


DEFAULT_IMAGES_DIR = pathlib.Path(__file__).parent.parent.parent / 'bench/data'


class ImageStorage:
    def __init__(self, images_dir: Union[str, pathlib.Path] = None):
        self._images_dir = pathlib.Path(images_dir) if images_dir else DEFAULT_IMAGES_DIR
        self._images = {name: self._read_image(name) for name in IMAGES.values()}

    def _read_image(self, name: str):
        with open(self._images_dir / name, 'rb') as f:
            return f.read()

    async def get_image(self, name):
        # emulates getting an image from another service for 50 ms
        await asyncio.sleep(0.05)
        return self._images[name]
