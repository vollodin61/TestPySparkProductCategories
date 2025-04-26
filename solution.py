import pandas as pd

# Создаем тестовые данные
products_data = [(1, "Продукт 1"), (2, "Продукт 2"), (3, "Продукт 3"),
                 (4, "Продукт 4"), (5, "Продукт 5")]
products_df = pd.DataFrame(products_data, columns=["product_id", "name"])

categories_data = [(1, "Категория 1"), (2, "Категория 2"), (3, "Категория 3")]
categories_df = pd.DataFrame(categories_data, columns=["category_id", "name"])

relations_data = [(1, 1), (1, 2), (2, 2), (3, 3)]  # Продукты 4 и 5 не имеют категорий
relations_df = pd.DataFrame(relations_data, columns=["product_id", "category_id"])

# Решение: соединяем таблицы (аналогично PySpark)
product_relations = pd.merge(
    products_df,
    relations_df,
    on="product_id",
    how="left"  # LEFT JOIN для включения продуктов без категорий
)

results = pd.merge(
    product_relations,
    categories_df,
    on="category_id",
    how="left"
)[["name_x", "name_y"]].rename(columns={"name_x": "product_name", "name_y": "category_name"})

# Показываем результат
print("Результат задания:")
print(results)

"""
# Эквивалентный код на PySpark (для демонстрации):
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProductCategoryDemo").getOrCreate()

# Создание DataFrame продуктов
products_data = [(1, "Продукт 1"), (2, "Продукт 2"), (3, "Продукт 3"),
                 (4, "Продукт 4"), (5, "Продукт 5")]
products_df = spark.createDataFrame(products_data, ["product_id", "name"])

# Создание DataFrame категорий
categories_data = [(1, "Категория 1"), (2, "Категория 2"), (3, "Категория 3")]
categories_df = spark.createDataFrame(categories_data, ["category_id", "name"])

# Создание DataFrame связей
relations_data = [(1, 1), (1, 2), (2, 2), (3, 3)]  # Продукты 4 и 5 не имеют категорий
relations_df = spark.createDataFrame(relations_data, ["product_id", "category_id"])

# Решение: соединяем таблицы
product_relations = products_df.join(
    relations_df,
    products_df.product_id == relations_df.product_id,
    "left"  # LEFT JOIN для включения продуктов без категорий
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
results.show()
"""
