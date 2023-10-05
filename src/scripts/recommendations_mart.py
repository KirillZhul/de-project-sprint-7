import sys
import datetime
import math
import pyspark

import findspark

findspark.init()
findspark.find()

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark.context import SparkContext

sc = SparkContext()
sql = SQLContext(sc)

date = sys.argv[1]
depth = sys.argv[2]
events_path = sys.argv[3]
cities_path = sys.argv[4]
target_path = sys.argv[5]

# date = "2022-05-31"
# depth = "30"
# events_path = f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/kirillzhul/data/geo/events/"
# cities_path = f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/kirillzhul/geo_2.csv"
# target_path = f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/kirillzhul/data/analytics/"


# функция для расчёта дистанции по координатам
def get_distance(lat_1, lat_2, long_1, long_2):
    lat_1 = (math.pi / 180) * lat_1
    lat_2 = (math.pi / 180) * lat_2
    long_1 = (math.pi / 180) * long_1
    long_2 = (math.pi / 180) * long_2
    return (
        2
        * 6371
        * math.asin(
            math.sqrt(
                math.pow(math.sin((lat_2 - lat_1) / 2), 2)
                + math.cos(lat_1)
                * math.cos(lat_2)
                * math.pow(math.sin((long_2 - long_1) / 2), 2)
            )
        )
    )


udf_func = F.udf(get_distance)


def input_paths(date, depth, events_path):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")

    return [
        f"{events_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"
        for x in range(int(depth))
    ]


def main():

    spark = (
        SparkSession.builder.master("yarn")
        .config("spark.driver.cores", "2")
        .config("spark.driver.memory", "16g")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
        .getOrCreate()
    )

    cities = (
        spark.read.csv(cities_path, sep=";", header=True)
        .withColumn("lat", regexp_replace("lat", ",", ".").cast(DoubleType()))
        .withColumn("lon", regexp_replace("lng", ",", ".").cast(DoubleType()))
        .withColumnRenamed("lat", "lat_c")
        .withColumnRenamed("lon", "lon_c")
        .select("id", "city", "lat_c", "lon_c", "timezone")
    )

    paths = input_paths(date, depth, events_path)

    events = spark.read.option("basePath", events_path).parquet(*paths)

    # определяем все подписки
    all_subscriptions = (
        events.filter(F.col("event_type") == "subscription")
        .where(
            (
                F.col("event.subscription_channel").isNotNull()
                & F.col("event.user").isNotNull()
            )
        )
        .select(
            F.col("event.subscription_channel").alias("channel_id"),
            F.col("event.user").alias("user_id"),
        )
        .distinct()
    )

    # распределяем пользователей в каналах на отправителей и получателей сообщений
    cols = ["user_left", "user_right"]
    subscriptions = (
        all_subscriptions.withColumnRenamed("user_id", "user_left")
        .join(
            all_subscriptions.withColumnRenamed("user_id", "user_right"),
            on="channel_id",
            how="inner",
        )
        .drop("channel_id")
        .filter(F.col("user_left") != F.col("user_right"))
        .withColumn("hash", F.hash(F.concat(F.col("user_left"), F.col("user_right"))))
    )

    # определяем отправителей сообщений
    users_messages_from_to = (
        events.filter("event_type == 'message'")
        .where(
            (
                F.col("event.message_from").isNotNull()
                & F.col("event.message_to").isNotNull()
            )
        )
        .select(
            F.col("event.message_from").alias("user_left"),
            F.col("event.message_to").alias("user_right"),
            F.col("lat").alias("lat_from"),
            F.col("lon").alias("lon_from"),
        )
        .distinct()
    )

    # определяем получателей сообщений
    users_messages_to_from = (
        events.filter("event_type == 'message'")
        .where(
            (
                F.col("event.message_from").isNotNull()
                & F.col("event.message_to").isNotNull()
            )
        )
        .select(
            F.col("event.message_to").alias("user_left"),
            F.col("event.message_from").alias("user_right"),
            F.col("lat").alias("lat_to"),
            F.col("lon").alias("lon_to"),
        )
        .distinct()
    )

    # определяем сообщения с временным форматом unix (секунды с начала эпохи)
    events_messages_unix = (
        events.filter("event_type == 'message'")
        .where(
            F.col("lat").isNotNull()
            | (F.col("lon").isNotNull())
            | (
                unix_timestamp(
                    F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss"
                ).isNotNull()
            )
        )
        .select(
            F.col("event.message_from").alias("user_right"),
            F.col("lat").alias("lat"),
            F.col("lon").alias("lon"),
            unix_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss").alias(
                "time"
            ),
        )
        .distinct()
    )

    w = Window.partitionBy("time")

    # определяем самые последние сообщения
    events_messages = (
        events_messages_unix.withColumn("maxdatetime", F.max("time").over(w))
        .where(F.col("time") == F.col("maxdatetime"))
        .drop("maxdatetime")
    )

    # находим все подписки с временным форматом unix
    events_subscription_unix = (
        events.filter("event_type == 'subscription'")
        .where(
            F.col("lat").isNotNull()
            | (F.col("lon").isNotNull())
            | (
                unix_timestamp(
                    F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss"
                ).isNotNull()
            )
        )
        .select(
            F.col("event.message_from").alias("user_right"),
            F.col("lat"),
            F.col("lon"),
            unix_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss").alias(
                "time"
            ),
        )
        .distinct()
    )

    # определяем самые последние подписки
    events_subscription = (
        events_subscription_unix.withColumn("maxdatetime", F.max("time").over(w))
        .where(F.col("time") == F.col("maxdatetime"))
        .drop("maxdatetime")
    )

    # объединяем последние подписки и сообщения
    events_coordinates = events_subscription.union(events_messages).distinct()

    # определяем пользователей контактировавших друг с другом
    users_intersection = (
        users_messages_from_to.union(users_messages_to_from)
        .withColumn("arr", F.array_sort(F.array(*cols)))
        .drop_duplicates(["arr"])
        .drop("arr")
        .withColumn("hash", F.hash(F.concat(F.col("user_left"), F.col("user_right"))))
        .filter(F.col("user_left") != F.col("user_right"))
    )

    # определяем пользователей не имевших друг с другом контактов
    subscriptions_without_intersection = (
        subscriptions.join(
            users_intersection.withColumnRenamed(
                "user_right", "user_right_temp"
            ).withColumnRenamed("user_left", "user_left_temp"),
            on=["hash"],
            how="left",
        )
        .where(F.col("user_right_temp").isNull())
        .drop("user_right_temp", "user_left_temp", "hash")
        .where(F.col("user_left") != 0)
        .filter(F.col("user_left") != F.col("user_right"))
    )

    # определяем последние координаты пользователей
    events_subscription_coordinates = subscriptions_without_intersection.join(
        events_coordinates.withColumnRenamed("user_id", "user_left")
        .withColumnRenamed("lon", "lon_left")
        .withColumnRenamed("lat", "lat_left"),
        on=["user_right"],
        how="inner",
    ).join(
        events_coordinates.withColumnRenamed("user_id", "user_right")
        .withColumnRenamed("lon", "lon_right")
        .withColumnRenamed("lat", "lat_right"),
        on=["user_right"],
        how="inner",
    )

    # определяем не контактирующих друг с другом пользователей в пределах одного города
    distance = (
        events_subscription_coordinates.withColumn(
            "distance",
            udf_func(
                F.col("lat_left"),
                F.col("lat_right"),
                F.col("lon_left"),
                F.col("lon_right"),
            ).cast(DoubleType()),
        )
        .where(F.col("distance") <= 1.0)
        .withColumnRenamed("lat_left", "lat")
        .withColumnRenamed("lon_left", "lon")
        .drop("lat_from", "lon_from", "distance")
    )

    # определяем город
    users_city = (
        distance.crossJoin(cities.hint("broadcast"))
        .withColumn(
            "distance",
            udf_func(F.col("lon"), F.col("lat"), F.col("lon_c"), F.col("lat_c")).cast(
                DoubleType()
            ),
        )
        .withColumn(
            "row",
            F.row_number().over(
                Window.partitionBy("user_left", "user_right").orderBy(
                    F.col("distance").asc()
                )
            ),
        )
        .filter(F.col("row") == 1)
        .drop("row", "lon", "lat", "city_lon", "city_lat", "distance", "channel_id")
        .withColumnRenamed("city", "zone_id")
        .distinct()
    )

    # вносим рекоммендации по подпискам пользователей
    recommendations = (
        users_city.withColumn("processed_dttm", current_date())
        .withColumn(
            "local_datetime",
            F.from_utc_timestamp(F.col("processed_dttm"), F.col("timezone")),
        )
        .withColumn("local_time", date_format(col("local_datetime"), "HH:mm:ss"))
        .select("user_left", "user_right", "processed_dttm", "zone_id", "local_time")
    )

    # записывем в формате parquet..
    recommendations.write.mode("overwrite").parquet(
        f"{target_path}/mart/recommendations/"
    )


if __name__ == "__main__":

    main()