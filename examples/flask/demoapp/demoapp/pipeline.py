import io
import json
import math
import pathlib
from contextlib import suppress
import logging
from dataclasses import dataclass
from typing import List, Tuple, Union

import torch
from PIL import Image
from timm.data.transforms import _pil_interp
from timm.models import create_model
from torchvision import transforms

DEVICE = 'cpu'  # can be 'cuda:0'
PROJ_ROOT = pathlib.Path(__file__).parent.parent
DATA_PATH = PROJ_ROOT / 'data'


@dataclass
class ModelInfo:
    name: str
    cfg: dict
    synset_path: str = DATA_PATH / 'imagenet_synsets.json'


EfficientNetS = ModelInfo(
    'tf_efficientnetv2_s',
    {
        'input_size': (3, 384, 384),
        'interpolation': 'bicubic',
        'mean': (0.5, 0.5, 0.5),
        'std': (0.5, 0.5, 0.5),
        'crop_pct': 1.0,
    },
)


class PreProcessor:
    def __init__(self, input_size: Union[int, Tuple[int, ...]], interpolation: str,
                 crop_pct: float, mean: Tuple[float, ...], std: Tuple[float, ...]):
        self.im_size = input_size if isinstance(input_size, int) else input_size[1]
        self.interpolation = interpolation
        self.crop_pct = crop_pct
        self.mean = mean
        self.std = std
        self.transform = self._get_transform()

    def process(self, im_bytes: bytes) -> torch.Tensor:
        im_pil = Image.open(io.BytesIO(im_bytes))
        return self.transform(im_pil)

    def _get_transform(self):
        scale_size = math.floor(self.im_size / self.crop_pct)
        return transforms.Compose([
            transforms.Resize(scale_size, _pil_interp(self.interpolation)),
            transforms.CenterCrop(self.im_size),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=torch.tensor(self.mean),
                std=torch.tensor(self.std),
            ), ])


class Classifier:
    def __init__(self, model_name: str, torchscript: bool, device: str,
                 model_dtype: torch.dtype, data_dtype: torch.dtype, use_amp: bool):
        self.model_name = model_name
        self.torchscript = torchscript
        self.device = device
        self.model_dtype = model_dtype
        self.data_dtype = data_dtype
        self.amp_autocast = torch.cuda.amp.autocast if use_amp else suppress

        logging.info('model initializing is started')
        self.model = self._get_model()
        logging.info('model initializing is finished')

    def process_list(self, data: List[torch.Tensor]) -> List[torch.Tensor]:
        batch = torch.stack(data)
        return [elem for elem in self._process_batch(batch)]

    def _get_model(self):
        model = create_model(
            self.model_name,
            num_classes=1000,
            in_chans=3,
            global_pool='fast',
            scriptable=self.torchscript,
            pretrained=True,
        )
        model.to(
            device=self.device,
            dtype=self.model_dtype,
        )
        if self.torchscript:
            model = torch.jit.script(model)
        model.eval()

        return model

    def _process_batch(self, batch: torch.Tensor) -> torch.Tensor:
        batch = batch.to(
            device=self.device,
            dtype=self.data_dtype,
        )
        with self.amp_autocast(), torch.no_grad():
            result = self.model(batch)
            result = result.to(device='cpu')

        return result


class PostProcessor:
    def __init__(self, synset_path: Union[pathlib.Path, str]):
        with open(synset_path, 'r') as fd:
            self.synset = json.loads(fd.read())

    def process(self, pred: torch.Tensor) -> List[Tuple[str, float]]:
        topk = torch.softmax(pred.to(dtype=torch.float32), axis=0).topk(5)  # noqa
        probs = topk.values.numpy().tolist()
        inds = topk.indices.numpy().tolist()
        names = [self.synset[str(ind)] for ind in inds]
        return list(zip(names, probs))


class Pipeline:
    def __init__(self, preprocessor: PreProcessor, classifier: Classifier,
                 postprocessor: PostProcessor):
        self.preprocessor = preprocessor
        self.classifier = classifier
        self.postprocessor = postprocessor

    def process(self, im: bytes) -> List[Tuple[str, float]]:
        # first step
        im_torch = self.preprocessor.process(im)
        # second step
        pred = self.classifier.process_list(data=[im_torch])[0]
        # third step
        named_predicts = self.postprocessor.process(pred)
        return named_predicts


class ModelsProducer:
    """Returns models for pipeline steps."""
    def __init__(self, model_info: ModelInfo):
        self._model_info = model_info

    def get_classifier(self) -> Classifier:
        return Classifier(
            model_name=self._model_info.name,
            torchscript=True,
            device=DEVICE,
            model_dtype=torch.float32,
            data_dtype=torch.float32,
            use_amp=False,
        )

    def get_pre_proc(self) -> PreProcessor:
        return PreProcessor(**self._model_info.cfg)

    def get_post_proc(self) -> PostProcessor:
        return PostProcessor(synset_path=self._model_info.synset_path)


default_producer = ModelsProducer(EfficientNetS)
