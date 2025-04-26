from pyspark.sql import DataFrame, SparkSession

from src.models.category import CategoryModel
from src.repositories.repository_interface import RepositoryInterface


class CategoryRepository(RepositoryInterface):
    """Репозиторий для работы с категориями."""

    def __init__(self, data_path=None):
        self.data_path = data_path
        self.model = CategoryModel()

    def get_all(self, spark: SparkSession) -> DataFrame:
        """Получает все категории."""
        if self.data_path:
            return spark.read.parquet(self.data_path)
        else:
            # Тестовые данные
            data = [
                (1, "Категория 1"),
                (2, "Категория 2"),
                (3, "Категория 3")
            ]
            return spark.createDataFrame(data, ["category_id", "name"])

    def get_by_id(self, spark: SparkSession, category_id) -> DataFrame:
        """Получает категорию по идентификатору."""
        categories = self.get_all(spark)
        return categories.filter(categories.category_id == category_id)
