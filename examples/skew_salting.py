# -*- coding: utf-8 -*-
"""Демонстрация salting против data skew в агрегации по перекошенному ключу."""
from pyspark.sql import SparkSession, functions as F


def run():
    spark = SparkSession.builder.appName("skew-salting").getOrCreate()

    # искусственный перекос: ключ 'hot' встречается в 100 раз чаще
    hot = spark.range(0, 1_000_000).withColumn("key", F.lit("hot")).withColumn("amount", F.rand())
    cold = spark.range(0, 10_000).withColumn(
        "key", F.concat(F.lit("k_"), (F.rand() * 50).cast("int"))
    ).withColumn("amount", F.rand())
    df = hot.unionByName(cold)

    # --- наивно (одна задача по 'hot' висит) ---
    naive = df.groupBy("key").agg(F.sum("amount").alias("amount"))

    # --- salting: агрегируем в два этапа ---
    N = 16
    salted = df.withColumn("salt", (F.rand() * N).cast("int"))
    stage1 = salted.groupBy("key", "salt").agg(F.sum("amount").alias("part"))
    salted_res = stage1.groupBy("key").agg(F.sum("part").alias("amount"))

    print("naive plan:");  naive.explain()
    print("salted plan:"); salted_res.explain()
    salted_res.orderBy("key").show(5)
    spark.stop()


if __name__ == "__main__":
    run()
