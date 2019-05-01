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

dim_store_product_sql = """
select * from rbu_sxcp_edw_dev.dim_store_product
"""
df_dim_store_product = spark.sql(dim_store_product_sql)
df_dim_store_product.createOrReplaceTempView("tmp_dim_store_product")

insert_rst_product = """
insert overwrite table rbu_sxcp_rst_dev.rst_product
(select
hash(a.store_code) store_pk,
a.store_code,
store_name,
hash(a.product_code) product_pk,
b.large_class,
a.product_code,
a.product_name,
a.unit,
b.warranty,
a.shelf_life,
a.sale_price,
a.purchase_price,
b.is_material,
b.is_valid,
a.min_order_qty,
a.min_display_qty,
a.safety_stock_qty,
b.fixed_days_supply,
NULL is_percent,
NULL max_modify_value,
NULL remark,
NULL extra1,
NULL extra2,
a.etl_time,
current_timestamp() update_time
from tmp_dim_store_product a
left join rbu_sxcp_edw_dev.dim_product b on a.product_code=b.product_code)
"""
print("开始插入rst_product")
spark.sql(insert_rst_product)
print("插入rst_product成功")

insert_rst_store = """
insert overwrite table rbu_sxcp_rst_dev.rst_store
(select 
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
from rbu_sxcp_edw_dev.dim_store)
"""
print("开始插入rst_store数据")
#spark.sql(insert_rst_store)
print("插入rst_store数据成功")

print("end----------------------------")
