from pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import datetime
import re
import time

spark = SparkSession.builder.master("yarn").appName("fct_sale_add").enableHiveSupport().getOrCreate()

sql_res = """
select * from rbu_sxcp_edw_ai_dev.store_sale_add where check_date<='2018-12-02'
"""
df_res = spark.sql(sql_res)
df_res.createOrReplaceTempView("res")
spark.sql("truncate table rbu_sxcp_edw_ai_dev.store_sale_add")
insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.store_sale_add partition(month)
(select
store_code,
product_code,
check_date,
sale_qty,
coalesce(sale_add_qty,0),
loss_qty,
sale_total_qty,
group_buying_qty,
end_qty,
etl_time,
month
from res 
where date_id=1)
DISTRIBUTE BY rand()
"""
print("开始插入数据")
start = time.time()
spark.sql(insert_sql)
end = time.time()
print("插入数据")
print('Running time: %s Seconds' % (end - start))

print("process successfully!")
print("=====================")
print("到此结束")
