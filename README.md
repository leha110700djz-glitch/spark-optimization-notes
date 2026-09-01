# spark-optimization-notes

![CI](https://github.com/leha110700djz-glitch/spark-optimization-notes/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/License-MIT-blue.svg)

Практические приёмы ускорения Spark/SQL-пайплайнов из реального опыта — с объяснением
«почему» и примерами кода. На таких приёмах я ускорял расчёт витрин **с ~2 часов до ~45 минут**.

## Содержание
1. [Как читать план и находить узкое место](#1-план)
2. [Shuffle — главный враг](#2-shuffle)
3. [Broadcast join](#3-broadcast-join)
4. [Data skew и salting](#4-data-skew)
5. [Партиционирование](#5-партиционирование)
6. [Кэширование](#6-кэширование)
7. [Форматы и predicate pushdown](#7-форматы)
8. [AQE](#8-aqe)
9. [Чек-лист оптимизации](#9-чек-лист)

## 1. План
Всё начинается с плана: `df.explain(True)` или Spark UI → вкладка SQL.
Ищем: `Exchange` (shuffle), `SortMergeJoin` на больших наборах, `Filter` после тяжёлых операций
(не протолкнулся вниз), spill to disk.

## 2. Shuffle
Shuffle = перераспределение данных между нодами при wide-операциях (join, groupBy, distinct,
repartition). Идёт по сети + запись на диск + (де)сериализация → главный источник тормозов.
**Правило:** уменьшай объём данных ДО shuffle (ранняя фильтрация/агрегация), избегай лишних
join, настраивай `spark.sql.shuffle.partitions` под объём (по умолчанию 200 — часто слишком много
для маленьких и мало для больших данных).

## 3. Broadcast join
Если одна из таблиц маленькая (< ~10–100 МБ) — рассылаем её на все ноды и убираем shuffle:
```python
from pyspark.sql.functions import broadcast
result = big_df.join(broadcast(small_df), "key")
```
Так `SortMergeJoin` (с shuffle обеих сторон) превращается в `BroadcastHashJoin` (без shuffle
большой стороны). Один из главных рычагов ускорения витрин.

## 4. Data skew
Перекос: у нескольких ключей на порядки больше строк → одна задача висит, остальные простаивают.
**Salting** — «разбавляем» горячий ключ случайным суффиксом и агрегируем в два этапа:
```python
from pyspark.sql import functions as F
N = 16
salted = df.withColumn("salt", (F.rand() * N).cast("int"))
stage1 = salted.groupBy("key", "salt").agg(F.sum("amount").alias("part"))
result = stage1.groupBy("key").agg(F.sum("part").alias("amount"))
```
Плюс: в Spark 3+ включить AQE skew join (см. §8). См. `examples/skew_salting.py`.

## 5. Партиционирование
- При записи: `df.write.partitionBy("dt").parquet(...)` — потребители читают только нужные партиции
  (partition pruning).
- В рантайме: `repartition(n, "key")` перед join по ключу; `coalesce(n)` чтобы уменьшить число
  файлов без полного shuffle.
- `repartition` (полный shuffle, можно увеличивать) vs `coalesce` (только уменьшает, дешевле).

## 6. Кэширование
Переиспользуешь DataFrame несколько раз в пайплайне → `df.cache()` (или `persist`), чтобы не
пересчитывать всю цепочку. Не кэшируй то, что используется один раз — зря съест память.

## 7. Форматы
Колоночные **Parquet/ORC** вместо CSV/JSON: сжатие, чтение только нужных колонок (column pruning),
**predicate pushdown** (фильтр применяется на уровне файла). Разница в разы по IO.

## 8. AQE
Adaptive Query Execution (Spark 3+) — на лету меняет план по реальной статистике:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```
Автоматически схлопывает лишние партиции, лечит skew join, меняет стратегию соединения.

## 9. Чек-лист
- [ ] Посмотрел план, нашёл Exchange/skew/spill.
- [ ] Отфильтровал и агрегировал ДО join.
- [ ] Маленькую таблицу — через `broadcast()`.
- [ ] Горячие ключи — salting + AQE skew join.
- [ ] Партиционирование при записи, pruning при чтении.
- [ ] Parquet/ORC вместо CSV/JSON.
- [ ] Кэш только для переиспользуемых DF.
- [ ] Включил AQE.
- [ ] Настроил `shuffle.partitions` под объём.

---

## Лицензия
MIT — см. [LICENSE](LICENSE).

## Автор
Alexey Chervak — Senior Data Engineer. Портфолио: https://github.com/leha110700djz-glitch
