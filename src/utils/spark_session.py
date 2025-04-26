from pyspark.sql import SparkSession


class SparkSessionFactory:
    """Фабрика для создания сессий Spark."""

    @staticmethod
    def create_session() -> SparkSession:
        """Создает и возвращает сессию Spark."""
        return SparkSession.builder \
            .appName("ProductCategoryApp") \
            .master("local[*]") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.driver.host", "localhost") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
            .config("spark.hadoop.security.authentication", "simple") \
            .config("spark.hadoop.security.authorization", "false") \
            .getOrCreate()
