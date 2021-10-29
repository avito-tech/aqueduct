from multiprocessing import (
    Process,
    Queue,
)
from multiprocessing.shared_memory import SharedMemory

import numpy as np
import pytest

from aqueduct.shm import (
    AtomicCounter,
    NPArraySharedData,
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


def update_task_field(q_in: Queue, q_out: Queue):
    task, field_name, value = q_in.get()
    array = getattr(task, field_name)
    array[0] = value
    q_out.put(None)


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
        # при присоединении памяти был увеличен счетчик ссылок
        assert shf_info.shm_wrapper.ref_count == 1

        q_in, q_out = Queue(), Queue()
        p = Process(target=handle_task, args=(q_out, q_in))
        p.start()

        q_out.put(task)
        q_in.get()
        # после присоединения области памяти дочерним процессом был увеличен счетчик ссылок
        assert shf_info.shm_wrapper.ref_count == 2

        q_out.put(None)
        q_in.get()
        # после работы gc в дочернем процессе счетчик ссылок был уменьшен
        assert shf_info.shm_wrapper.ref_count == 1

        q_out.put(None)
        p.join()

    def test_save_shared_value_directly(self, task: ArrayFieldTask):
        # НЕ рекомендуемый способ расшарить значение поля, но так тоже можно
        field_name = 'array'

        shared_array = NPArraySharedData.create_from_data(task.array)
        task.array = shared_array
        assert field_name in task._shared_fields
        assert task._shared_fields[field_name].shm_wrapper.ref_count == 1


class TestAtomicCounter:
    def test_size(self):
        assert AtomicCounter.size() == 4

    def test_init(self):
        view = memoryview(bytearray((1, 0, 0, 0)))
        AtomicCounter(view)

    def test_inc(self):
        view = memoryview(bytearray([0] * 16))
        c = AtomicCounter(view)

        assert c.inc() == 1
        assert c.inc() == 2
        assert c.dec() == 1
        assert c.dec() == 0

    @staticmethod
    def process_atomic(input):
        myshm = SharedMemory(name=input[0])
        myc = AtomicCounter(myshm.buf)

        if input[1] % 2 == 0:
            for _ in range(10000):
                myc.inc()
        if input[1] % 2 != 0:
            for _ in range(10000):
                myc.dec()

    def test_multiproc_atomic(self):
        """Modify counter in multiple processes.

        There is equal amount of increments/decrements - so result should be 0
        """
        from multiprocessing import Pool
        shm = SharedMemory(create=True, size=1024 * 1024 + AtomicCounter.size())
        c = AtomicCounter(shm.buf)

        pool = Pool(10)
        pool.map(self.process_atomic, [(shm.name, i) for i in range(10)])

        assert c.get() == 0
