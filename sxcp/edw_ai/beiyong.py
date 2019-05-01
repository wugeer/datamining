from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, UDFRegistration, SparkSession
from pyspark.sql.functions import udf
import datetime
import re
import time

spark = SparkSession.builder.master("yarn").appName("sale_feat").enableHiveSupport().getOrCreate()

sql_sale_feat = """
select 
row_number() over(partition by store_code,product_code,sale_date order by sale_date desc) date_id,
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
from rbu_sxcp_edw_ai_dev.sale_feat
"""
df_sale_feat = spark.sql(sql_sale_feat)
df_sale_feat.createOrReplaceTempView("sale_feat")
# df_sale_feat.dropDuplicates()
print("starting..........")
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
spark.sql("set hive.exec.reducers.bytes.per.reducer=1024000000")
insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_feat partition(month)
(select 
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
from sale_feat
where date_id=1)
DISTRIBUTE BY rand()
"""
spark.sql(insert_sql)
print("插入数据成功")
df_sale_feat.drop()
print("删除临时表成功")
print("process successfully!")
print("=====================")




