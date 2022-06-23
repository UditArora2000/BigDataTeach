import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkConf
import time

spark = (SparkSession \
    .builder \
    .appName("BDA") \
    .config("spark.jars", "/home/akshala/Downloads/postgresql-42.2.19.jre6.jar") \
    .master("spark://localhost:7077") \
    .config('spark.executor.cores', '4') \
    .getOrCreate())

start_time = time.time()

df1a = spark.read.jdbc(url = "jdbc:postgresql://localhost:5432/bda", 
                     table = "(with mindate as (select min(date) as mind from pullreq), maxdate as (select max(date) as maxd from pullreq) select date, coalesce(count, 0) from (select alld.date::date from generate_series((select * from mindate), (select * from maxdate), '1 day') as alld(date)) alld left join ( select status, date, count(distinct pid) from pullreq where status='opened' group by status, date order by date) ans using (date) order by date) AS my_table",
                     properties={"user": "postgres", "password": "bda", "driver": 'org.postgresql.Driver'}).createTempView('tbl1a')

df1b = spark.read.jdbc(url = "jdbc:postgresql://localhost:5432/bda", 
                     table = "(with mindate as (select min(date) as mind from pullreq), maxdate as (select max(date) as maxd from pullreq) select date, coalesce(count, 0) from (select alld.date::date from generate_series((select * from mindate), (select * from maxdate), '1 day') as alld(date)) alld left join ( select status, date, count(distinct pid) from pullreq where status='discussed' group by status, date order by date) ans using (date) order by date) AS my_table",
                     properties={"user": "postgres", "password": "bda", "driver": 'org.postgresql.Driver'}).createTempView('tbl1b')

df2 = spark.read.jdbc(url = "jdbc:postgresql://localhost:5432/bda", 
                     table = "(with month_com as (select name, count(*), date_part('month', date) as month from pullreq where status='discussed' group by month, name) select month, count, name from month_com as M where count = (select max(count) from month_com as N where M.month=N.month) order by month) AS my_table",
                     properties={"user": "postgres", "password": "bda", "driver": 'org.postgresql.Driver'}).createTempView('tbl2')

df3 = spark.read.jdbc(url = "jdbc:postgresql://localhost:5432/bda", 
                     table = "(with week_com as (select name, count(*), date_part('year', date) as year, date_part('week', date) as week from pullreq where status='discussed' group by year, week, name) select year, week, name, count from week_com as M where count=(select max(count) from week_com as N where M.year=N.year and M.week=N.week) order by year, week) AS my_table",
                     properties={"user": "postgres", "password": "bda", "driver": 'org.postgresql.Driver'}).createTempView('tbl3')

df4 = spark.read.jdbc(url = "jdbc:postgresql://localhost:5432/bda", 
                     table = "(select date_part('year', date) as year, date_part('week', date) as week, count(distinct pid) from pullreq where status='opened' group by year, week order by year, week) AS my_table",
                     properties={"user": "postgres", "password": "bda", "driver": 'org.postgresql.Driver'}).createTempView('tbl4')

df5 = spark.read.jdbc(url = "jdbc:postgresql://localhost:5432/bda", 
                     table = "(select month, coalesce(count, 0) from (select allm.month from generate_series(1, 12) as allm(month)) allm left join ( select date_part('month', date) as month, count(distinct pid) from pullreq where status = 'merged' and date_part('year', date) = 2010 group by month order by month) ans using (month) order by month) AS my_table",
                     properties={"user": "postgres", "password": "bda", "driver": 'org.postgresql.Driver'}).createTempView('tbl5')

df6 = spark.read.jdbc(url = "jdbc:postgresql://localhost:5432/bda", 
                     table = "(with mindate as (select min(date) as mind from pullreq), maxdate as (select max(date) as maxd from pullreq) select date, coalesce(count, 0) from (select alld.date::date from generate_series((select * from mindate), (select * from maxdate), '1 day') as alld(date)) alld left join ( select date, count(*) from pullreq group by date order by date) ans using (date) order by date) AS my_table",
                     properties={"user": "postgres", "password": "bda", "driver": 'org.postgresql.Driver'}).createTempView('tbl6')

df7 = spark.read.jdbc(url = "jdbc:postgresql://localhost:5432/bda", 
                     table = "(select name, count(distinct pid) from pullreq where status = 'opened' and date_part('year', date) = 2011 group by name order by count(distinct pid) desc limit 1) AS my_table",
                     properties={"user": "postgres", "password": "bda", "driver": 'org.postgresql.Driver'}).createTempView('tbl7')

print(f"Computation time: {time.time() - start_time}")

spark.sql('select * from tbl1a').show()
spark.sql('select * from tbl1b').show()
spark.sql('select * from tbl2').show()
spark.sql('select * from tbl3').show()
spark.sql('select * from tbl4').show()
spark.sql('select * from tbl5').show()
spark.sql('select * from tbl6').show()
spark.sql('select * from tbl7').show()