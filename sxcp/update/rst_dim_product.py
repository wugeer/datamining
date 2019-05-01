# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import time
from datetime import datetime, date, timedelta
import re
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
# import sys

spark = SparkSession.builder.master("yarn").appName("fct_pos_detail").enableHiveSupport().getOrCreate()
tmp1_sql = """
SELECT hash(product_code) sk,
large_class,
product_code,
product_name, 
unit,
warranty, 
shelf_life, 
is_material, 
is_valid, 
min_order_qty, 
NULL min_display_qty,
min_package_qty,
safety_stock_qty, 
NULL supply_lead_time,
NULL without_supply_weekday,
fixed_days_supply, 
NULL remark,
NULL extra1,
NULL extra2,
etl_time,
current_timestamp() update_time
FROM rbu_sxcp_edw_dev.dim_product
"""
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1 = df_tmp1.dropDuplicates()
df_tmp1.createOrReplaceTempView("tmp1")


insert_sql = """
insert overwrite table rbu_sxcp_rst_dev.rst_dim_product
(select * from tmp1)
"""
spark.sql(insert_sql)
print("插入数据成功")
print("process successfully!")
print("=====================")


