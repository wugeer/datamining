# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import time
from datetime import datetime, date, timedelta

spark = SparkSession.builder.master("yarn").appName("fct_store_product_on_sale").enableHiveSupport().getOrCreate()
item = date.today().strftime("%Y-%m-%d")

print("starting..........")
tmp1_sql = """
SELECT  store_code, product_code, sale_price
FROM rbu_sxcp_ods_dev.rst_store_product
where stop_cargo_date is null or stop_cargo_date<'{0}'
""".format(item)
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1")

insert_sql = """
insert overwrite table rbu_sxcp_edw_dev.fct_store_product_on_sale
(select store_code, product_code, sale_price,current_timestamp etl_time from tmp1)
"""
print("开始插入数据")
spark.sql(insert_sql)
print("插入数据成功")

print("删除缓存表成功")
print("process successfully!")
print("=====================")
