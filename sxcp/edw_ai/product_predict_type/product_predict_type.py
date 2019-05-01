# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, UDFRegistration, SparkSession
from pyspark.sql.functions import udf
import datetime
import re
import time

spark = SparkSession.builder.master("yarn").appName("product_predict_type").enableHiveSupport().getOrCreate()

print("starting..........")
item = '2019-01-02'
sql_on_sale_history = """
SELECT store_code, product_code, sale_date
FROM rbu_sxcp_edw_dev.fct_store_product_on_sale_history
"""
df_on_sale_history = spark.sql(sql_on_sale_history)
df_on_sale_history = df_on_sale_history.dropDuplicates()
df_on_sale_history.createOrReplaceTempView("store_product_on_sale_history")

tmp1_sql = """
select store_code,
product_code,
count(*) times
from store_product_on_sale_history
where sale_date>=date_sub('{0}', 365) and sale_date<='{0}'
group by store_code,product_code
""".format(item)
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1")

tmp2_sql = """
select store_code,
product_code,
case when times>=28 then 1
     else 0
end predict_type
from tmp1
"""
df_tmp2 = spark.sql(tmp2_sql)
df_tmp2.createOrReplaceTempView("tmp2")

insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.product_predict_type
(select
store_code,
product_code,
predict_type,
'{0}',
current_timestamp()
from tmp2)
""".format(item)
spark.sql(insert_sql)
df_tmp1.drop()
df_tmp2.drop()
df_on_sale_history.drop()

print("process successfully!")
print("=====================")

