from pyspark.sql import SparkSession
import os

# Устанавливаем эти переменные перед созданием SparkSession
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"  # Укажите путь к Java 11, если она установлена

# Создаем максимально простую сессию Spark
spark = SparkSession.builder \
    .appName("ProductCategoryDemo") \
    .master("local[*]") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

try:
    # Создаем тестовые данные
    products_data = [(1, "Продукт 1"), (2, "Продукт 2"), (3, "Продукт 3"),
                     (4, "Продукт 4"), (5, "Продукт 5")]
    products_df = spark.createDataFrame(products_data, ["product_id", "name"])

    categories_data = [(1, "Категория 1"), (2, "Категория 2"), (3, "Категория 3")]
    categories_df = spark.createDataFrame(categories_data, ["category_id", "name"])

    relations_data = [(1, 1), (1, 2), (2, 2), (3, 3)]  # Продукты 4 и 5 не имеют категорий
    relations_df = spark.createDataFrame(relations_data, ["product_id", "category_id"])

    # Решение: соединяем таблицы
    product_relations = products_df.join(
        relations_df,
        products_df.product_id == relations_df.product_id,
        "left"
    )

    results = product_relations.join(
        categories_df,
        product_relations.category_id == categories_df.category_id,
        "left"
    ).select(
        products_df.name.alias("product_name"),
        categories_df.name.alias("category_name")
    )

    # Показываем результат
    print("Результат задания:")
    results.show()

finally:
    # Закрываем сессию
    spark.stop()