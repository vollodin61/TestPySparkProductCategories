from pyspark.sql import DataFrame, SparkSession

from src.models.product import ProductModel
from src.repositories.repository_interface import RepositoryInterface


class ProductRepository(RepositoryInterface):
    """Репозиторий для работы с продуктами."""

    def __init__(self, data_path=None):
        self.data_path = data_path
        self.model = ProductModel()

    def get_all(self, spark: SparkSession) -> DataFrame:
        """Получает все продукты."""
        if self.data_path:
            return spark.read.parquet(self.data_path)
        else:
            # Тестовые данные
            data = [
                (1, "Продукт 1"),
                (2, "Продукт 2"),
                (3, "Продукт 3"),
                (4, "Продукт 4"),
                (5, "Продукт 5")
            ]
            return spark.createDataFrame(data, ["product_id", "name"])

    def get_by_id(self, spark: SparkSession, product_id) -> DataFrame:
        """Получает продукт по идентификатору."""
        products = self.get_all(spark)
        return products.filter(products.product_id == product_id)
