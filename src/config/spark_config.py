from pyspark import SparkConf


class SparkConfig:
    """Класс для конфигурации Spark."""

    @staticmethod
    def get_config() -> SparkConf:
        """Возвращает конфигурацию Spark."""
        conf = SparkConf()
        conf.setAppName("ProductCategoryApp")
        conf.setMaster("local[*]")
        conf.set("spark.executor.memory", "2g")
        conf.set("spark.driver.memory", "2g")
        return conf
