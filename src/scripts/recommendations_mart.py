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
    
    spark = SparkSession.builder \
                    .master("yarn") \
                    .config("spark.driver.cores", "2") \
                    .config("spark.driver.memory", "16g") \
                    .config("spark.dynamicAllocation.enabled", "true") \
                    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
                    .getOrCreate()
    
    
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
    
    # расчёт дистанций между пользователями и городами
    def get_subs_city(common_subs_distance, cities_path, spark):
        messages_cities = (
            common_subs_distance.crossJoin(cities)
            .withColumn(
                "distance",
                udf_func(
                    F.col("lat_1"), F.col("lat_c"), F.col("lon_1"), F.col("lon_c")
                ).cast(DoubleType()),
            )
            .withColumn(
                "distance_rank",
                F.row_number().over(
                    Window().partitionBy(["user_left"]).orderBy(F.asc("distance"))
                ),
            )
            .where("distance_rank == 1")
            .drop("distance_rank", "distance")
            .withColumnRenamed("city", "zone_id")
        )

        return messages_cities

    def get_common_subs_distance_zone(events, cities_path, spark):

        # рассчитываем все пары пользователей, которые подписаны на один канал
        subs_user_left = (
            events.filter(F.col("event_type") == "subscription")
            .select(
                F.col("event.user").alias("user_left"),
                F.col("lat").alias("lat_1").cast(DoubleType()),
                F.col("lon").alias("lon_1").cast(DoubleType()),
                "event.subscription_channel",
            )
            .filter("lat_1 is not null and lon_1 is not null")
        )

        subs_user_right = (
            events.filter(F.col("event_type") == "subscription")
            .select(
                F.col("event.user").alias("user_right"),
                F.col("lat").alias("lat_2").cast(DoubleType()),
                F.col("lon").alias("lon_2").cast(DoubleType()),
                "event.subscription_channel",
            )
            .filter("lat_2 is not null and lon_2 is not null")
        )

        common_subs = (
            subs_user_left.join(subs_user_right, "subscription_channel", "inner")
            .where(F.col("user_left") != F.col("user_right"))
            .distinct()
        )

        # рассчитываем все пары пользователей, которые подписаны на один канал  и находятся менее, чем на 1 км друг от друга
        common_subs_distance = common_subs.withColumn(
            "distance",
            udf_func(
                F.col("lat_1"), F.col("lat_2"), F.col("lon_1"), F.col("lon_2")
            ).cast(DoubleType()),
        ).where((F.col("distance").isNotNull()) & (F.col("distance") < 1.0))

        # рассчитываем через функцию get_subs_city города (zone_id)
        common_subs_distance_zone = get_subs_city(
            common_subs_distance, cities_path, spark
        )

        return common_subs_distance_zone

    # рассчитываем необходимые датасеты с помощью функций
    common_subs_distance_zone1 = get_common_subs_distance_zone(
        events, cities_path, spark
    )

    # рассчитываем все пары пользователей, которые переписывались
    user_left_contacts = events.where(F.col("event_type") == "message").select(
        col("event.message_from").alias("user_left"),
        col("event.message_to").alias("user_right"),
    )

    user_right_contacts = events.where(F.col("event_type") == "message").select(
         col("event.message_to").alias("user_left"),
         col("event.message_from").alias("user_right"),
    )
        
    real_contacts = user_left_contacts.union(user_right_contacts).distinct()
                
    # определяем пользователей в пределах одного города, не имеющих контактов друг с другом
    no_contacts_users = common_subs_distance_zone1.join(
        real_contacts, ["user_left", "user_right"], "left_anti"
    )

    # рассчитываем финальный датасет для пользователей, которым можно предложить рекоммендации
    recommendations = (
        no_contacts_users.withColumn("processed_dttm", current_date())
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
