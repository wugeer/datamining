from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, UDFRegistration, SparkSession
from pyspark.sql.functions import udf
import datetime
import re
import time

spark = SparkSession.builder.master("yarn").appName("mid_turnover_feat").enableHiveSupport().getOrCreate()

print("starting..........")
tmp1_sql = """
SELECT store_code, sale_date, date_case, turnover, pre, temp, recent_3, recent_7, etl_time
FROM rbu_sxcp_edw_ai_dev.turnover_feat_test
"""
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1 = df_tmp1.dropDuplicates()
df_tmp1.createOrReplaceTempView("tmp1")
insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.turnover_feat
(select * from tmp1)
"""
spark.sql(insert_sql)
print("process successfully!")
print("=====================")
print("到此结束")
