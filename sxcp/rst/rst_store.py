# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf
import datetime
import re
import pandas as pd
#from datetime import datetime, date, timedelta
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
# import sys

spark = SparkSession.builder.master("yarn").appName("rst_product").enableHiveSupport().getOrCreate()

print("start-----------------------------")
dim_store_sql = """
select * from rbu_sxcp_edw_dev.dim_store
"""
df_dim_store = spark.sql(dim_store_sql)
df_dim_store.createOrReplaceTempView("tmp_dim_store")

insert_dim_store = """
insert overwrite table rbu_sxcp_rst_dev.rst_store
(select 
monotonically_increasing_id() id,
hash(store_code) store_pk,
store_code,
store_name,
store_class,
business_class,
area area_name,
NULL area_code,
province province_name,
NULL province_code,
city city_name,
NULL city_code,
country_district county_district_name,
NULL county_district_code,
longitude,
latitude,
distribute_route,
is_valid,
opening_date,
phone_number,
order_cycle,
address,
parent_org_code,
open_time,
close_time,
is_open,
NULL remark,
NULL extra1,
NULL extra2,
etl_time,
current_timestamp()
from tmp_dim_store)
"""

print("开始插入")
spark.sql(insert_dim_store)
print("插入成功")

print("end----------------------------")
