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

tmp1_sql = """
SELECT store_code, product_code, sale_date, qty, d1, d2, d3, d4, d5, d6, d7, error1, error2, error3, error4,
 error5, error6, error7, `month`
FROM rbu_sxcp_edw_ai_dev.sale_predict_test2
where store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') and qty is not null
"""
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1 = df_tmp1.dropDuplicates()
df_tmp1.createOrReplaceTempView("tmp1")

spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
spark.sql("set hive.exec.reducers.bytes.per.reducer=1024000000")
insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_predict_test2 partition(month)
(select 
store_code,
product_code,  
sale_date,
qty, 
d1,
d2, 
d3, 
d4, 
d5, 
d6, 
d7, 
error1, 
error2, 
error3, 
error4, 
error5, 
error6, 
error7,
month
from tmp1)
"""
spark.sql(insert_sql)
print("插入数据成功")
print("process successfully!")
print("=====================")

