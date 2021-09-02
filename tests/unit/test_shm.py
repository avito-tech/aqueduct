from multiprocessing import Process, Queue
from unittest.mock import patch

import numpy as np
import pytest

from aqueduct.shm import (
    NPArraySharedData,
    SharedMemoryWrapper,
)
from tests.unit.conftest import Task


class ArrayFieldTask(Task):
    def __init__(self, array: np.array, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.array = array


def update_sh_array(q_in: Queue, q_out: Queue):
    array_sh_data, value = q_in.get()
    sh_array = array_sh_data.get_data()
    sh_array[0] = value
    q_out.put((None, None))
    # дожидаемся изменения массива в главном процессе
    _, new_value = q_in.get()
    # теперь первый элемент массива равен new_value
    # сама проверка делается в родительском процессе, потому что исключение из дочернего не прокидывается
    # в родительский процесс автоматически
    q_out.put((sh_array[0] == new_value, f'array elem {sh_array[0]} is not equal to value {new_value}'))


def update_ref_counter(q_in: Queue, q_out: Queue):
    array_sh_data, value = q_in.get()
    shm_wrapper: SharedMemoryWrapper = array_sh_data.shm_wrapper
    ref_counter = shm_wrapper._ref_counter  # noqa
    ref_counter[0] = value
    q_out.put((None, None))
    # дожидаемся изменения ref_counter в главном процессе
    _, new_value = q_in.get()
    # теперь ref_counter равен new_value
    q_out.put((ref_counter[0] == new_value, f'ref_count {ref_counter[0]} is not equal to value {new_value}'))


def update_task_field(q_in: Queue, q_out: Queue):
    task, field_name, value = q_in.get()
    array = getattr(task, field_name)
    array[0] = value
    q_out.put(None)


def close_access_to_shm(q_in: Queue, q_out: Queue):
    array_sh_data: NPArraySharedData = q_in.get()
    # closes access to shm
    array_sh_data.shm_wrapper.shm.close()
    q_out.put(None)
    q_in.get()
    # error when trying to use shm
    try:
        array_sh_data.get_data()
    except ValueError as e:
        q_out.put(e)


@pytest.fixture
def task(array: np.ndarray):
    return ArrayFieldTask(array)


def handle_task(q_in: Queue, q_out: Queue):
    # была присоединена разделяемая память
    task = q_in.get()
    q_out.put(None)

    # объект, который ссылался на резделяемую память, должен быть удален gc
    task = q_in.get()
    q_out.put(None)

    q_in.get()


class TestNPArraySharedData:
    def test_access_to_data(self, array_sh_data: NPArraySharedData):
        sh_array = array_sh_data.get_data()

        q_in, q_out = Queue(), Queue()
        p = Process(target=update_sh_array, args=(q_out, q_in))
        p.start()

        value = sh_array[0] + 1
        assert sh_array[0] != value
        q_out.put((array_sh_data, value))
        # дожидаемся изменения массива в дочернем процессе
        _, _ = q_in.get()
        # теперь первый элемент массива равен value
        assert sh_array[0] == value
        value += 1
        sh_array[0] = value
        q_out.put((None, value))
        assert_expr, msg = q_in.get()
        assert assert_expr, msg

        p.join()

    def test_access_to_ref_counter(self, array_sh_data: NPArraySharedData):
        ref_counter = array_sh_data.shm_wrapper._ref_counter

        q_in, q_out = Queue(), Queue()
        p = Process(target=update_ref_counter, args=(q_out, q_in))
        p.start()

        value = 4
        q_out.put((array_sh_data, value))
        # дожидаемся изменения массива в дочернем процессе
        _, _ = q_in.get()
        # теперь ref_counter равен value
        assert ref_counter[0] == value
        value = 2
        ref_counter[0] = value
        q_out.put((None, value))
        assert_expr, msg = q_in.get()
        assert assert_expr, msg

        p.join()

    # todo добавить тесты на подсчет ссылок и удаление объектов, unlink памяти, переприсваивание массива, SharedData
    def test_access_to_shm_after_close(self, array_sh_data: NPArraySharedData):
        q_in, q_out = Queue(), Queue()
        p = Process(target=close_access_to_shm, args=(q_out, q_in))
        p.start()

        # creates array successfully in parent process
        sh_array = array_sh_data.get_data()
        sh_array[0] = 42
        # send shm data to child process
        q_out.put(array_sh_data)
        # access to shm were closed in child process
        q_in.get()
        # creates array successfully in parent process
        sh_array = array_sh_data.get_data()
        assert sh_array[0] == 42
        q_out.put(None)
        # error in child process when trying to use shm
        exc = q_in.get()
        assert isinstance(exc, ValueError)

        p.join()

        # creates array successfully after child process is finished
        sh_array = array_sh_data.get_data()
        assert sh_array[0] == 42


class TestSharedFieldsMixin:
    def test_pickle_task(self, task: ArrayFieldTask):
        field_name = 'array'

        state = task.__getstate__()
        assert field_name in state
        assert state['_shared_fields'] == {}

        # если поле расшарено, то оно не пиклится, вместо него пиклится мета информация
        task.share_value(field_name)
        state = task.__getstate__()
        assert field_name not in state
        assert field_name in state['_shared_fields']

    def test_update_task_field(self, task: ArrayFieldTask):
        task.share_value('array')

        q_in, q_out = Queue(), Queue()
        p = Process(target=update_task_field, args=(q_out, q_in))
        p.start()

        value = task.array[0] + 1
        q_out.put((task, 'array', value))
        # дожидаемся изменения массива в дочернем процессе
        _ = q_in.get()
        # теперь первый элемент массива равен value
        assert task.array[0] == value

        p.join()

    def test_auto_change_ref_counter(self, task: ArrayFieldTask):
        task.share_value('array')
        shf_info = task._shared_fields['array']
        ref_counter = shf_info.shm_wrapper._ref_counter
        # при присоединении памяти был увеличен счетчик ссылок
        assert ref_counter[0] == 1

        q_in, q_out = Queue(), Queue()
        p = Process(target=handle_task, args=(q_out, q_in))
        p.start()

        q_out.put(task)
        q_in.get()
        # после присоединения области памяти дочерним процессом был увеличен счетчик ссылок
        assert ref_counter[0] == 2

        q_out.put(None)
        q_in.get()
        # после работы gc в дочернем процессе счетчик ссылок был уменьшен
        assert ref_counter[0] == 1

        q_out.put(None)
        p.join()

        task.array = None
        with patch.object(shf_info.shm_wrapper.shm, 'unlink') as mocked:
            shf_info, ref_counter = None, None
            # после того, как не остается ссылок на разделяемую память, она освобождается
            assert mocked.called is True

    def test_save_shared_value_directly(self, task: ArrayFieldTask):
        # НЕ рекомендуемый способ расшарить значение поля, но так тоже можно
        field_name = 'array'

        shared_array = NPArraySharedData.create_from_data(task.array)
        task.array = shared_array
        assert field_name in task._shared_fields
        assert task._shared_fields[field_name].shm_wrapper._ref_counter[0] == 1
