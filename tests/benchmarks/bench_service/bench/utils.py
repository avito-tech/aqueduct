import subprocess


def guess_number(gte: int, lt: int):
    left, right = gte, lt
    number = left

    while True:
        val = (yield number)
        if val is not None:
            right = val
            break
        else:
            left = number
            number *= 2

    if number == gte:
        return

    while True:
        number = (left + right) // 2
        if number == left:
            break
        val = (yield number)
        if val is not None:
            right = val
        elif number > left:
            left = number

    yield number


class MaxNumberFinder:
    def __init__(self, gte: int = 1, lt: int = 10**3):
        self._numbers = guess_number(gte, lt)
        self.number = next(self._numbers)
        self.max_number = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type == StopIteration:
            return True

    def next(self, less: bool = False):
        if less:
            self.number = self._numbers.send(self.number)
        else:
            self.max_number = self.number
            self.number = next(self._numbers)

        if self.number == self.max_number:
            raise StopIteration


def run_cmd(command: str, stdout=None, shell=False, wait: bool = True) -> subprocess.Popen:
    cmd = command if shell else command.split()
    p = subprocess.Popen(cmd, stdout=stdout, shell=shell)
    if wait:
        p.wait()
    return p
