from .task import BaseTask


class BaseTaskHandler:
    """Базовый класс для ваших хендлеров.
       Используется для логики которой не требуется модель и специальная инициализация.
       Если же нужна модель, которая долго стартует и занимает память, то используйте ModelTaskHandler()."""

    @classmethod
    def get_step_name(cls, step_number: int) -> str:
        return f'step{step_number}_{cls.__name__}'

    def on_start(self):
        """Вызывается при старте в дочернем процессе. Чтобы модель занимала память только в подпроцессе,
        а не в родителе."""
        pass

    def handle(self, *tasks: BaseTask):
        """Вызывается при получении из очереди задания."""
        raise NotImplementedError
