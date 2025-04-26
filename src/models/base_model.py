from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class BaseModel(ABC):
    """Абстрактный базовый класс для моделей данных."""

    @property
    @abstractmethod
    def schema(self):
        """Схема данных модели."""
        pass

    @abstractmethod
    def validate(self, df: DataFrame) -> bool:
        """Проверяет соответствие DataFrame схеме модели."""
        pass
