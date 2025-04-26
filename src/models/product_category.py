from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType

from src.models.base_model import BaseModel


class ProductCategory(BaseModel):
    """Модель связи между продуктом и категорией."""

    @property
    def schema(self):
        return StructType([
            StructField("product_id", IntegerType(), False),
            StructField("category_id", IntegerType(), False)
        ])

    def validate(self, df: DataFrame) -> bool:
        try:
            for field in self.schema:
                if field.name not in df.columns:
                    return False
            return True
        except Exception as e:
            print(f"Ошибка: {repr(e)}")
            return False
