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
#spark.sql("drop table rbu_sxcp_edw_ai_dev.store_sale_add")
tmp1_sql = """
SELECT store_code, sale_date, time_3_turnover, time_4_turnover, time_5_turnover, time_6_turnover, time_7_turnover, time_8_turnover, time_9_turnover, time_10_turnover, time_11_turnover, time_12_turnover, time_13_turnover, time_14_turnover, time_15_turnover, time_16_turnover, time_17_turnover, time_18_turnover, time_19_turnover, time_20_turnover, time_21_turnover, time_22_turnover, time_23_turnover, time_0_turnover, time_1_turnover, time_2_turnover
FROM rbu_sxcp_tmp.rst_turnover_feat_sum
"""
#where check_date<'2019-01-13'
# where store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659')
df_tmp1 = spark.sql(tmp1_sql)
#df_tmp1.show()
#df_tmp1 = df_tmp1.dropDuplicates()
df_tmp1.createOrReplaceTempView("tmp1")

spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
#spark.sql("truncate table rbu_sxcp_edw_ai_dev.store_sale_add")
insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.turnover_feat_sum partition(year_month)
(select 
 store_code, sale_date, time_3_turnover, time_4_turnover, time_5_turnover, time_6_turnover, time_7_turnover, 
 time_8_turnover, time_9_turnover, time_10_turnover, time_11_turnover, time_12_turnover, time_13_turnover, 
 time_14_turnover, time_15_turnover, time_16_turnover, time_17_turnover, time_18_turnover, time_19_turnover, 
 time_20_turnover, time_21_turnover, time_22_turnover, time_23_turnover, time_0_turnover, time_1_turnover, 
 time_2_turnover,substring(sale_date,1,7) 
from tmp1)
"""
spark.sql(insert_sql)
print("插入数据成功")
print("process successfully!")
print("=====================")

