# -*- coding: utf-8 -*-
"""Демонстрация broadcast join: SortMergeJoin -> BroadcastHashJoin (без shuffle большой стороны)."""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast


def run():
    spark = SparkSession.builder.appName("broadcast-join").getOrCreate()

    big = spark.range(0, 50_000_000).withColumn(
        "merchant_id", (F.rand() * 1000).cast("int")).withColumn("amount", F.rand() * 100)
    dim = spark.range(0, 1000).withColumnRenamed("id", "merchant_id").withColumn(
        "category", F.concat(F.lit("cat_"), (F.col("merchant_id") % 10)))

    # без подсказки Spark может выбрать SortMergeJoin (shuffle обеих сторон)
    smj = big.join(dim, "merchant_id")
    print("=== без broadcast ==="); smj.explain()

    # с broadcast маленькой стороны -> BroadcastHashJoin, без shuffle большой таблицы
    bhj = big.join(broadcast(dim), "merchant_id")
    print("=== с broadcast ==="); bhj.explain()

    (bhj.groupBy("category").agg(F.round(F.sum("amount"), 2).alias("revenue"))
        .orderBy("category").show(10))
    spark.stop()


if __name__ == "__main__":
    run()
