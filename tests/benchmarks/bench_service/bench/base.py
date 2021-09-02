import csv
import json
from dataclasses import dataclass
from typing import Callable, List


IMAGES = {
    'xs': 'apple.jpg',
    's': 'apples.jpg',  # small
    'm': 'cat.jpg',
    'l': 'umbrella.jpg',
}


@dataclass
class RpsCounterCfg:
    replicas: int
    nprocs: List[int]
    container_ids: List[str]
    image_name: str
    timeout: float
    load_time_secs: int
    success_rate: float
    min_rps: int


def get_rps_counter_cfg(cfg_path: str) -> RpsCounterCfg:
    with open(cfg_path) as f:
        cfg = json.loads(f.read())
    return RpsCounterCfg(**cfg)


@dataclass
class ReportRow:
    rps: int
    success_rate: float
    replicas: int
    nprocs: List[int]
    max_GPU_UTIL: float
    mean_GPU_UTIL: float
    image_name: str
    load_time_secs: int
    is_max_rps: bool = False

    @classmethod
    def get_field_names(cls):
        return list(cls.__dataclass_fields__.keys())

    @classmethod
    def get_from_csv(
            cls,
            csv_name: str,
            condition: Callable[['ReportRow'], bool] = lambda x: True,
    ) -> List['ReportRow']:
        lst = []
        with open(csv_name) as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['is_max_rps'] = row['is_max_rps'].lower()
                image_name = row.pop('image_name')
                report_row = ReportRow(**{k: json.loads(v) for k, v in row.items()}, image_name=image_name)
                if condition(report_row):
                    lst.append(report_row)
        return lst
