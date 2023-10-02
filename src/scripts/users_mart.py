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


from pyspark.sql.functions import *
from pyspark.sql.functions import udf


date = sys.argv[1]
depth = sys.argv[2]
events_path = sys.argv[3]
cities_path = sys.argv[4]
target_path = sys.argv[5]

#date = '2022-05-31'
#depth = '30'
#events_path = f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/kirillzhul/data/geo/events/"
#cities_path = f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/kirillzhul/geo_2.csv"
#target_path = f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/kirillzhul/data/analytics/"

spark = (
    SparkSession.builder.master("local")
    .appName("Learning DataFrames")
    .getOrCreate()
)

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


cities = (
        spark.read.csv(cities_path, sep=";", header=True)
        .withColumn("lat", regexp_replace("lat", ",", ".").cast(DoubleType()))
        .withColumn("lon", regexp_replace("lng", ",", ".").cast(DoubleType()))
        .withColumnRenamed("lat", "lat_c")
        .withColumnRenamed("lon", "lon_c")
        .select("id", "city", "lat_c", "lon_c", "timezone")
)

   
def get_general_tab(events_messages, cities_path, spark):

    messages_cities = (
        events_messages.crossJoin(cities)
        .withColumn(
            "distance",
            udf_func(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lon_c")).cast(
                "float"
            ),
        )
        .withColumn(
            "distance_rank",
            F.row_number().over(
                Window()
                .partitionBy(["user_id", "message_id"])
                .orderBy(F.asc("distance"))
            ),
        )
        .where("distance_rank == 1")
        .drop("distance_rank", "distance", "lat", "lon", "id", "lat_c", "lon_c")
    )
    return messages_cities  

    
def get_act_city(events_messages, csv_path, spark):

    messages_cities = (
        events_messages.withColumn(
            "datetime_rank",
            F.row_number().over(
                Window().partitionBy(["user_id"]).orderBy(F.desc("datetime"))
            ),
        )
        .where("datetime_rank == 1")
        .orderBy("user_id")
        .crossJoin(cities)
        .withColumn(
            "distance",
            udf_func(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lon_c")).cast(
                "float"
            ),
        )
        .withColumn(
            "distance_rank",
            F.row_number().over(
                Window().partitionBy(["user_id"]).orderBy(F.asc("distance"))
            ),
        )
        .where("distance_rank == 1")
        .select("user_id", F.col("city").alias("act_city"), "date", "timezone")
    )

    return messages_cities    

def main():

    spark = (
        SparkSession.builder.master("local")
        .appName("Learning DataFrames")
        .getOrCreate()
    )

    paths = input_paths(date, depth, events_path)

    events = spark.read.option("basePath", events_path).parquet(*paths)

    # вычисляем датасет со всеми сообщениями
    events_messages = (
        events.where(F.col("event_type") == "message")
        .withColumn(
            "date",
            F.date_trunc(
                "day",
                F.coalesce(F.col("event.datetime"), F.col("event.message_ts")),
            ),
        )
        .selectExpr(
            "event.message_from as user_id",
            "event.message_id",
            "date",
            "event.datetime",
            "lat",
            "lon",
        )
    )

    # вычисляем датасет со всеми сообщениями за заданный период
    general_tb = get_general_tab(events_messages, cities_path, spark)

    # вычисляем датасет с городом, из которого было отправлено последнее сообщение
    act_cities_df = get_act_city(events_messages, cities_path, spark)

    # рассчитываем таблицу с изменениями города отправки сообщения
    temp_df = (
        general_tb.withColumn(
            "max_date", F.max("date").over(Window().partitionBy("user_id"))
        )
        .withColumn(
            "city_lag",
            F.lead("city", 1, "empty").over(
                Window().partitionBy("user_id").orderBy(F.col("date").desc())
            ),
        )
        .filter(F.col("city") != F.col("city_lag"))
    )

    # рассчитываем адрес города, из которого были отправлены 27 дней подряд сообщения от пользователя
    home_city = (
        temp_df.withColumnRenamed("city", "home_city")
        .withColumn(
            "date_lag",
            F.coalesce(
                F.lag("date").over(
                    Window().partitionBy("user_id").orderBy(F.col("date").desc())
                ),
                F.col("max_date"),
            ),
        )
        .withColumn("date_diff", F.datediff(F.col("date_lag"), F.col("date")))
        .where(F.col("date_diff") > 27)
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(F.col("date").desc())
            ),
        )
        .where(F.col("rank") == 1)
        .drop("date_diff", "date_lag", "max_date", "city_lag", "rank")
    )

    # рассчитываем кол-во смен города по каждому пользователю
    travel_count = (
        temp_df.groupBy("user_id").count().withColumnRenamed("count", "travel_count")
    )

    # рассчитываем список городов, которые посетил пользователь
    travel_list = temp_df.groupBy("user_id").agg(
        F.collect_list("city").alias("travel_array")
    )

    # рассчитываем локальное время
    time_local = act_cities_df.withColumn(
        "localtime", F.from_utc_timestamp(F.col("date"), F.col("timezone"))
    ).drop("timezone", "city", "date", "datetime", "act_city")

    # объединяем все данные в одну витрину
    final = (
        act_cities_df.select("user_id", "act_city")
        .join(home_city, "user_id", "left")
        .join(travel_count, "user_id", "left")
        .join(travel_list, "user_id", "left")
        .join(time_local, "user_id", "left")
        .select(
            "user_id",
            "act_city",
            "home_city",
            "travel_count",
            "travel_array",
            "localtime",
        )
    )

    # записываем результат по заданному пути
    final.write.mode("overwrite").parquet(
        f"{target_path}/mart/users/_{date}_{depth}"
    )


if __name__ == "__main__":

    main()