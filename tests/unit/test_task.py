from typing import Dict
from unittest.mock import Mock

from tests.unit.conftest import Task
from aqueduct.shm import SharedData, SharedMemoryWrapper
from aqueduct.task import BaseTask


class IntSharedData(SharedData):
    def __init__(self, shm_wrapper: SharedMemoryWrapper, value: int):
        super().__init__(shm_wrapper)
        self.value = value

    def get_data(self) -> int:
        return self.value

    @classmethod
    def create_from_data(cls, data: int) -> 'IntSharedData':
        return cls(Mock(), data)


def get_task_with_shf(shf_data: Dict[str, SharedData]):
    t = BaseTask()
    for k, v in shf_data.items():
        setattr(t, k, v)
    return t


class TestBaseTask:
    def test_get_queues_info(self, task: Task):
        task.result = 'some result'
        task2 = Task()
        task2.update(task)

        assert task2.task_id != task.task_id, 'Id must be different'
        assert task2.result == task.result, 'Result must be same'
        assert task2.metrics is not task.metrics

    def test_update_shared_fields_source_task_with_shf(self):
        target_task = BaseTask()
        source_task = get_task_with_shf({
            'foo': IntSharedData.create_from_data(1),
            'bar': IntSharedData.create_from_data(2),
        })
        target_task.update(source_task)

        assert target_task._shared_fields == source_task._shared_fields
        assert getattr(target_task, 'foo') == getattr(source_task, 'foo')
        assert getattr(target_task, 'bar') == getattr(source_task, 'bar')

    def test_update_shared_fields_both_tasks_with_shf(self):
        target_task = get_task_with_shf({
            'foo': IntSharedData.create_from_data(1),
            'bar': IntSharedData.create_from_data(2),
        })
        source_task = get_task_with_shf({
            'foo': IntSharedData.create_from_data(3),
            'baz': IntSharedData.create_from_data(4),
        })
        old_tt_shf_info = target_task._shared_fields
        st_shf_info = source_task._shared_fields
        target_task.update(source_task)

        new_tt_shf_info = target_task._shared_fields

        assert new_tt_shf_info['foo'] == old_tt_shf_info['foo']
        assert new_tt_shf_info['bar'] == old_tt_shf_info['bar']
        assert new_tt_shf_info['baz'] == st_shf_info['baz']

        assert getattr(target_task, 'foo') == 1
        assert getattr(target_task, 'bar') == 2
        assert getattr(target_task, 'baz') == 4
