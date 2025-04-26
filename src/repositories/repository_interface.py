from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class RepositoryInterface(ABC):
    """Интерфейс для репозиториев данных."""

    @abstractmethod
    def get_all(self, spark: SparkSession) -> DataFrame:
        """Получает все записи."""
        pass

    @abstractmethod
    def get_by_id(self, spark: SparkSession, entity_id) -> DataFrame:
        """Получает запись по идентификатору."""
        pass
