from pyspark.sql import DataFrame, SparkSession

from src.models.product_category import ProductCategory
from src.repositories.repository_interface import RepositoryInterface


class RelationRepository(RepositoryInterface):
    """Репозиторий для работы со связями продуктов и категорий."""

    def __init__(self, data_path=None):
        self.data_path = data_path
        self.model = ProductCategory()

    def get_all(self, spark: SparkSession) -> DataFrame:
        """Получает все связи."""
        if self.data_path:
            return spark.read.parquet(self.data_path)
        else:
            # Тестовые данные
            data = [
                (1, 1),  # Продукт 1 - Категория 1
                (1, 2),  # Продукт 1 - Категория 2
                (2, 2),  # Продукт 2 - Категория 2
                (3, 3)  # Продукт 3 - Категория 3
                # Продукт 4 и 5 не имеют категорий
            ]
            return spark.createDataFrame(data, ["product_id", "category_id"])

    def get_by_id(self, spark: SparkSession, relation_id) -> DataFrame:
        """Не применимо к связям (переопределено для соответствия интерфейсу)."""
        raise NotImplementedError("Метод get_by_id не применим к связям")

    def get_by_product_id(self, spark: SparkSession, product_id) -> DataFrame:
        """Получает связи по идентификатору продукта."""
        relations = self.get_all(spark)
        return relations.filter(relations.product_id == product_id)

    def get_by_category_id(self, spark: SparkSession, category_id) -> DataFrame:
        """Получает связи по идентификатору категории."""
        relations = self.get_all(spark)
        return relations.filter(relations.category_id == category_id)
