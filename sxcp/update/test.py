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

spark = SparkSession.builder.master("yarn").appName("sale_predict").enableHiveSupport().getOrCreate()
print("开始跑sale_predict")
tmp1_sql = """
SELECT shop_code, product_code, check_date, shelf_life, end_qty, digital_warehouse
FROM rbu_sxcp_tmp.rst_digital_warehouse
"""
df_tmp1 = spark.sql(tmp1_sql)
#df_tmp1 = df_tmp1.dropDuplicates()
df_tmp1.createOrReplaceTempView("tmp1")

spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
spark.sql("set hive.exec.reducers.bytes.per.reducer=1024000000")
insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.digital_warehouse partition(year_month)
(select 
shop_code, product_code, check_date, shelf_life, end_qty, digital_warehouse,current_timestamp(),
substring(check_date,1,7)
from tmp1)
"""
spark.sql(insert_sql)
print("插入数据成功")
print("process successfully!")
print("=====================")

