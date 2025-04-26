from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from src.models.base_model import BaseModel


class ProductModel(BaseModel):
    """Модель продукта."""

    @property
    def schema(self):
        return StructType([
            StructField("product_id", IntegerType(), False),
            StructField("name", StringType(), False)
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
