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
from pyspark.sql.functions import expr
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

# date = '2022-05-31'
# depth = '30'
# events_path = f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/kirillzhul/data/geo/events/"
# cities_path = f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/kirillzhul/geo_2.csv"
# target_path = f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/kirillzhul/data/analytics/"


cols = [
    "month",
    "week",
    "zone_id",
    "week_message",
    "week_reaction",
    "week_subscription",
    "week_user",
    "month_message",
    "month_reaction",
    "month_subscription",
    "month_user",
]

spark = SparkSession.builder \
                    .master("yarn") \
                    .config("spark.driver.cores", "2") \
                    .config("spark.driver.memory", "16g") \
                    .config("spark.dynamicAllocation.enabled", "true") \
                    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
                    .getOrCreate()

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


# геоданные городов, без timezone
cities = (
    spark.read.csv(cities_path, sep=";", header=True)
    .withColumn("lat", regexp_replace("lat", ",", ".").cast(DoubleType()))
    .withColumn("lon", regexp_replace("lng", ",", ".").cast(DoubleType()))
    .withColumnRenamed("lat", "lat_c")
    .withColumnRenamed("lon", "lon_c")
    .select("id", "city", "lat_c", "lon_c")  # timezone здесь уже не интересует..
)


def main():

    spark = (
        SparkSession.builder.master("local")
        .appName("Learning DataFrames")
        .getOrCreate()
    )

    paths = input_paths(date, depth, events_path)

    events = spark.read.option("basePath", events_path).parquet(*paths)

    # подсчёт всех сообщений
    all_messages = (
        events.where(F.col("event_type") == "message")
        .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
        .select(
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_id").alias("message_id"),
            "lat",
            "lon",
            F.to_date(
                F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))
            ).alias("date"),
        )
    )

    # подсчёт всех реакций на сообщения
    all_reactions = (
        events.where(F.col("event_type") == "reaction")
        .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
        .select(
            F.col("event.message_id").alias("message_id"),
            "lat",
            "lon",
            F.to_date(
                F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))
            ).alias("date"),
        )
    )

    # подсчёт описаний
    all_subscriptions = (
        events.where(F.col("event_type") == "subscription")
        .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
        .select(
            F.col("event.user").alias("user_id"),
            "lat",
            "lon",
            F.to_date(
                F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))
            ).alias("date"),
        )
    )

    # привязка координат пользователей, отправивших сообщения, к геоданным городов
    all_messages_and_citygeodata = all_messages.crossJoin(cities.hint("broadcast"))

    # привязка координат пользователей, отправивших реакции на сообщения, к геоданным городов
    all_reactions_and_citygeodata = all_reactions.crossJoin(cities.hint("broadcast"))

    # привязка координат пользователей, выполнивших подписку, к геоданным городов
    all_subscriptions_and_citygeodata = all_subscriptions.crossJoin(
        cities.hint("broadcast")
    )

    # подсчёт дистанции по координатам пользователей, отправивших сообщения, относительно данных по геоданным городов
    distance_messages = all_messages_and_citygeodata.withColumn(
        "distance",
        udf_func(F.col("lat"), F.col("lon"), F.col("lat_c"), F.col("lon_c")).cast(
            DoubleType()
        ),
    )

    # подсчёт дистанции по координатам пользователей, отреагировавших на сообщения, относительно данных по геоданным городов
    distance_reactions = all_reactions_and_citygeodata.withColumn(
        "distance",
        udf_func(F.col("lat"), F.col("lon"), F.col("lat_c"), F.col("lon_c")).cast(
            DoubleType()
        ),
    )

    # подсчёт дистанции по координатам пользователей, выполнивших подписку, относительно данных по геоданным городов
    distance_subscriptions = all_subscriptions_and_citygeodata.withColumn(
        "distance",
        udf_func(F.col("lat"), F.col("lon"), F.col("lat_c"), F.col("lon_c")).cast(
            DoubleType()
        ),
    )

    # определение ближайшего города, из которого было отправлено самое последнее сообщение
    nearest_city_message = (
        distance_messages.withColumn(
            "row",
            F.row_number().over(
                Window.partitionBy("user_id", "date", "message_id").orderBy(
                    F.col("distance").asc()
                )
            ),
        )
        .filter(F.col("row") == 1)
        .select("user_id", "message_id", "date", "id", "city")
        .withColumnRenamed("city", "zone_id")
    )

    # определение ближайшего города, из которого было отправлена самоя последняя реакция на сообщение
    nearest_city_reaction = (
        distance_reactions.withColumn(
            "row",
            F.row_number().over(
                Window.partitionBy("message_id", "date", "lat", "lon").orderBy(
                    F.col("distance").asc()
                )
            ),
        )
        .filter(F.col("row") == 1)
        .select("message_id", "date", "id", "city")
        .withColumnRenamed("city", "zone_id")
    )

    # определение ближайшего города, из которого было произведена последнея подписка
    nearest_city_subscription = (
        distance_subscriptions.withColumn(
            "row",
            F.row_number().over(
                Window.partitionBy("lat", "lon", "date").orderBy(
                    F.col("distance").asc()
                )
            ),
        )
        .filter(F.col("row") == 1)
        .select("user_id", "date", "id", "city")
        .withColumnRenamed("city", "zone_id")
    )

    # расчёт всех сообщений по определённому выше городу с разбивкой по неделям и месяцам
    count_messages = (
        nearest_city_message.withColumn("month", month(F.col("date")))
        .withColumn(
            "week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd"))
        )
        .withColumn(
            "week_message",
            (F.count("message_id").over(Window.partitionBy("zone_id", "week"))),
        )
        .withColumn(
            "month_message",
            (F.count("message_id").over(Window.partitionBy("zone_id", "month"))),
        )
        .select("zone_id", "week", "month", "week_message", "month_message")
        .distinct()
    )

    # расчёт всех регистраций по определённому выше городу с разбивкой по неделям и месяцам
    count_registrations = (
        nearest_city_message.withColumn("month", month(F.col("date")))
        .withColumn(
            "week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd"))
        )
        .withColumn(
            "row",
            (
                F.row_number().over(
                    Window.partitionBy("user_id").orderBy(F.col("date").asc())
                )
            ),
        )
        .filter(F.col("row") == 1)
        .withColumn(
            "week_user", (F.count("row").over(Window.partitionBy("zone_id", "week")))
        )
        .withColumn(
            "month_user", (F.count("row").over(Window.partitionBy("zone_id", "month")))
        )
        .select("zone_id", "week", "month", "week_user", "month_user")
        .distinct()
    )

    # расчёт всех реакций на собщения по определённому выше городу с разбивкой по неделям и месяцам
    count_reactions = (
        nearest_city_reaction.withColumn("month", month(F.col("date")))
        .withColumn(
            "week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd"))
        )
        .withColumn(
            "week_reaction",
            (F.count("message_id").over(Window.partitionBy("zone_id", "week"))),
        )
        .withColumn(
            "month_reaction",
            (F.count("message_id").over(Window.partitionBy("zone_id", "month"))),
        )
        .select("zone_id", "week", "month", "week_reaction", "month_reaction")
        .distinct()
    )

    # расчёт всех подписок по определённому выше городу с разбивкой по неделям и месяцам
    count_subscriptions = (
        nearest_city_subscription.withColumn("month", month(F.col("date")))
        .withColumn(
            "week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd"))
        )
        .withColumn(
            "week_subscription",
            (F.count("user_id").over(Window.partitionBy("zone_id", "week"))),
        )
        .withColumn(
            "month_subscription",
            (F.count("user_id").over(Window.partitionBy("zone_id", "month"))),
        )
        .select("zone_id", "week", "month", "week_subscription", "month_subscription")
        .distinct()
    )

    # расчёт суммарных данных по всем сообщениям, регистрациям, реакциям  и подпискам
    # по определённому выше городу с разбивкой по неделям и месяцам (за указанный период)
    geo_analitics_mart = (
        count_messages.join(
            count_registrations, ["zone_id", "week", "month"], how="full"
        )
        .join(count_reactions, ["zone_id", "week", "month"], how="full")
        .join(count_subscriptions, ["zone_id", "week", "month"], how="full")
    )

    # Заполняем пропуски значением по умолчанию
    geo_analitics_mart = geo_analitics_mart.fillna(0)
    
    geo_analitics_mart.write.mode("overwrite").parquet(f"{target_path}/mart/geo")

if __name__ == "__main__":

    main()
