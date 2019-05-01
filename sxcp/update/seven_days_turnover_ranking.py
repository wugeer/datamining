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

item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
print("开始生成%s 七天销量排名数据" % item)

pos_detail_sql = """
SELECT store_code, product_code, qty, discounted_price, amt, sale_date
FROM rbu_sxcp_edw_dev.fct_pos_detail
where sale_date>='2018-08-20'
"""
df_pos_detail = spark.sql(pos_detail_sql)
df_pos_detail.createOrReplaceTempView("pos_detail")
#spark.catalog.cacheTable("pos_detail")

store_product_sql = """
select store_code,product_code,sale_date from rbu_sxcp_edw_dev.fct_on_sale_history 
where sale_date='{0}'
""".format(item)
df_store_product = spark.sql(store_product_sql)
df_store_product = df_store_product.dropDuplicates()
df_store_product.createOrReplaceTempView("store_product")
#spark.catalog.cacheTable("store_product")

tmp1_sql = """
select a.store_code, a.product_code, b.amt,b.qty,b.discounted_price
from store_product a 
left join pos_detail b on a.store_code=b.store_code and a.product_code=b.product_code and b.sale_date>date_sub('{0}', 7) and b.sale_date<='{0}'
""".format(item)
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1")

tmp2_sql = """
select store_code, product_code, sum(amt) turnover, sum(qty) qty, avg(discounted_price) discounted_price
from tmp1
group by store_code, product_code
"""
df_tmp2 = spark.sql(tmp2_sql)
df_tmp2.createOrReplaceTempView("tmp2")

tmp3_sql = """
select
row_number() over(partition by store_code order by turnover desc) amt_id,
store_code,
product_code,
turnover,
qty,
discounted_price
from tmp2
"""
df_tmp3 = spark.sql(tmp3_sql)
df_tmp3.createOrReplaceTempView("tmp3")
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')

union_sql = """
SELECT store_code,
product_code, 
sale_date, 
sale_total_qty, 
sale_price, 
turnover, 
rank, 
etl_time, 
year_month
FROM rbu_sxcp_rst_dev.seven_days_turnover_ranking
where sale_date<'{0}'
union 
select
store_code,
product_code,
'{0}' sale_date,
qty sale_total_qty,
discounted_price sale_price,
turnover,
amt_id rank,
current_timestamp() etl_time,
substring('{0}', 1, 7) year_month
from tmp3
""".format(item)
df_union = spark.sql(union_sql)
df_union.createOrReplaceTempView("union_tmp")

insert_sql = """
insert overwrite table rbu_sxcp_rst_dev.seven_days_turnover_ranking partition(year_month)
(select
store_code,
product_code, 
sale_date, 
sale_total_qty, 
sale_price, 
turnover, 
rank, 
etl_time, 
year_month
from union_tmp)
"""
print("开始插入数据")
start = time.time()
spark.sql(insert_sql)
end = time.time()
print('Running time: %s Seconds' % (end - start))
print("插入数据成功")

print("释放缓存表成功")
print("process successfully!")
print("=====================")

