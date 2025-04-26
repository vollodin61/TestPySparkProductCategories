from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class ServiceInterface(ABC):
    """Интерфейс для сервисов данных."""

    @abstractmethod
    def process_data(self, spark: SparkSession) -> DataFrame:
        """Обрабатывает данные."""
        pass
