import sys
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

def main():
        base_input_path=sys.argv[1]
        output_path=sys.argv[2]

        #base_input_path = '/user/master/data/geo/events/'
        #output_path = '/user/kirillzhul/data/geo/events/'

        conf = SparkConf().setAppName(f"test1")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

#чтение данных
        events = sql.read.parquet(f"{base_input_path}").sample(0.1)

# запись данных
        events\
        .write\
        .mode('overwrite')\
        .partitionBy(['date', 'event_type'])\
        .format('parquet')\
        .save(f"{output_path}")


if __name__ == "__main__":
        main()
        
#! /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/initial_load.py /user/master/data/geo/events/ /user/kirillzhul/data/geo/events/