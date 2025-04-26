from pyspark.sql import DataFrame, SparkSession

from src.repositories.category_repository import CategoryRepository
from src.repositories.product_repository import ProductRepository
from src.repositories.relation_repository import RelationRepository
from src.services.service_interface import ServiceInterface


class ProductCategoryService(ServiceInterface):
    """Сервис для работы с продуктами и категориями."""

    def __init__(
            self,
            product_repo: ProductRepository,
            category_repo: CategoryRepository,
            relation_repo: RelationRepository
    ):
        self.product_repo = product_repo
        self.category_repo = category_repo
        self.relation_repo = relation_repo

    def process_data(self, spark: SparkSession) -> DataFrame:
        """
        Обрабатывает данные для получения всех пар «Имя продукта – Имя категории»
        и имен всех продуктов, у которых нет категорий.
        """
        return self.get_product_category_pairs(spark)

    def get_product_category_pairs(self, spark: SparkSession) -> DataFrame:
        """
        Получает пары «Имя продукта – Имя категории» и имена всех продуктов, 
        у которых нет категорий.
        """
        # Получаем все продукты, категории и их связи
        products_df = self.product_repo.get_all(spark)
        categories_df = self.category_repo.get_all(spark)
        relations_df = self.relation_repo.get_all(spark)

        # Объединяем продукты и связи (левое соединение, чтобы включить продукты без категорий)
        product_relations = products_df.join(
            relations_df,
            products_df.product_id == relations_df.product_id,
            "left"
        )

        # Объединяем результат с категориями
        results = product_relations.join(
            categories_df,
            product_relations.category_id == categories_df.category_id,
            "left"
        ).select(
            products_df.name.alias("product_name"),
            categories_df.name.alias("category_name")
        )

        return results
