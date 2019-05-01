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
item = date.today().strftime("%Y-%m-%d")
print("开始插入%s sale_predict数据"%item)

spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
spark.sql("set hive.exec.reducers.bytes.per.reducer=1024000000")
insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_predict partition(sale_date)
(SELECT shop_code, product_code, NULL qty, d1, d2, d3, d4, d5, d6, d7,NULL error1,NULL error2,NULL error3,NULL error4,
NULL error5,NULL error6,NULL error7,sale_date FROM rbu_sxcp_edw_ai_dev.sale_pre_result)
""".format(item)
spark.sql(insert_sql)
print("插入数据成功")
print("process successfully!")
print("=====================")

