# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import datetime
import re
import time
import pandas as pd
#from datetime import datetime, date, timedelta
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

spark = SparkSession.builder.master("yarn").appName("fct_sale_add").enableHiveSupport().getOrCreate()

print("starting..........")
chang_sql = """
SELECT row_number() over(partition by store_code,sale_date order by sale_date desc) date_id,
store_code, 
sale_date, 
date_case, 
turnover, 
pre, 
temp, 
recent_3, 
recent_7, 
etl_time, 
month
FROM rbu_sxcp_edw_ai_dev.turnover_feat_test
"""
df_turnover_feat = spark.sql(chang_sql)
df_turnover_feat.createOrReplaceTempView("turnover_feat")
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
spark.sql("set hive.exec.reducers.bytes.per.reducer=1024000000")
insert_turnover_feat = """
insert into table rbu_sxcp_edw_ai_dev.turnover_feat partition(month)
(SELECT 
a.store_code, 
a.sale_date, 
b.case, 
turnover, 
pre, 
temp, 
recent_3, 
recent_7, 
etl_time, 
month
FROM turnover_feat a
left join rbu_sxcp_edw_ai_dev.store_category b on a.store_code=b.shop_code and a.sale_date=b.date
where date_id=1)
"""
print("开始插入替换case后的turnover_feat数据")
spark.sql("truncate table rbu_sxcp_edw_ai_dev.turnover_feat")
spark.sql(insert_turnover_feat)
print("插入成功")


chang2_sql = """
SELECT row_number() over(partition by store_code,product_code,sale_date order by sale_date desc) date_id,
store_code, 
product_code, 
sale_date, 
date_case, 
turnover, 
temp, 
qty, 
recent_3, 
recent_7, 
etl_time, 
month
FROM rbu_sxcp_edw_ai_dev.sale_feat_test
"""
df_sale_feat = spark.sql(chang2_sql)
df_sale_feat.createOrReplaceTempView("sale_feat")

insert_sale_feat = """
insert into table rbu_sxcp_edw_ai_dev.sale_feat partition(month)
(SELECT 
a.store_code, 
product_code, 
a.sale_date, 
b.case, 
turnover, 
temp, 
qty, 
recent_3, 
recent_7, 
etl_time, 
month
FROM sale_feat a
left join rbu_sxcp_edw_ai_dev.store_category b on a.store_code=b.shop_code and a.sale_date=b.date
where date_id=1)
"""
print("开始插入替换case后的sale_feat数据")
spark.sql("truncate table rbu_sxcp_edw_ai_dev.sale_feat")
spark.sql(insert_sale_feat)
print("插入成功")


print("process successfully!")
print("=====================")
print("到此结束")

