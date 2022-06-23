import findspark
findspark.init()

import pandas as pd
from pyspark.sql import SparkSession
import os
import spark_df_query as sdq

import time

spark = (SparkSession \
    .builder \
    .appName("BDA") \
    .config("spark.jars", "/home/akshala/Downloads/postgresql-42.2.19.jre6.jar") \
    .master("spark://localhost:7077") \
    .config('spark.executor.cores', '4') \
    .getOrCreate())

df = spark.read.format("jdbc"). \
options(
         url='jdbc:postgresql://localhost:5432/bda', 
         dbtable='pullreq',
         user='postgres',
         password='bda',
         driver='org.postgresql.Driver').\
load().registerTempTable("pullreq")

start_time = time.time()

a1_a = spark.sql(sdq.q1_a)

a1_b = spark.sql(sdq.q1_b)

_ = spark.sql(sdq.month_com).registerTempTable("month_com")

a2 = spark.sql(sdq.q2)

_ = spark.sql(sdq.week_com).registerTempTable("week_com")

a3 = spark.sql(sdq.q3)

a4 = spark.sql(sdq.q4)

a5 = spark.sql(sdq.q5)

a6 = spark.sql(sdq.q6)

a7 = spark.sql(sdq.q7)

print(f"Computation time: {time.time()-start_time}")

# a1_a.show()
# a1_b.show()
# a2.show()
# a3.show()
# a4.show()
# a5.show()
# a6.show()
# a7.show()