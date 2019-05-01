# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import datetime

spark = SparkSession.builder.master("yarn").appName("fct_store_product_on_sale_history").enableHiveSupport().getOrCreate()

print("starting..........")
sql_on_sale = """
SELECT store_code, 
store_name, 
product_code, 
product_name, 
product_type, 
unit, 
sale_price, 
purchase_price, 
shelf_life, 
min_order_qty, 
min_display_qty, 
etl_time
FROM rbu_sxcp_edw_dev.fct_store_product_on_sale
"""
df_on_sale = spark.sql(sql_on_sale)
df_on_sale.createOrReplaceTempView("on_sale")

sql_tmp1 = """
select product_code,
case when warranty=1 then 1
     when warranty=2 then 1
     when warranty=3 then 1
     when warranty=4 then 1
     when warranty=5 then 2
     when warranty=6 then 3
     when warranty=7 then 4
     else warranty
end best_life
from rbu_sxcp_edw_dev.dim_product
"""
df_tmp1 = spark.sql(sql_tmp1)
df_tmp1.createOrReplaceTempView("tmp1")

sql_tmp2 = """
select product_code,
case when product_code='pr_code_1421' then 1
     when product_code='pr_code_0564' then 1
     when product_code='pr_code_1269' then 1
     else best_life
end best_life
from tmp1
"""
df_tmp2 = spark.sql(sql_tmp2)
df_tmp2.createOrReplaceTempView("tmp2")

insert_sql = """
insert into table rbu_sxcp_edw_dev.fct_store_product_on_sale_history
(select 
store_code,
store_name,
a.product_code,
product_name,
product_type,
to_date(etl_time),
sale_price, 
purchase_price,
shelf_life,
unit,
min_order_qty, 
min_display_qty, 
b.best_life
from on_sale a
left join tmp2 b on a.product_code=b.product_code)
"""
print("开始插入on_sale_history")
spark.sql(insert_sql)
print("插入on_sale_history成功")
df_on_sale.drop()
df_tmp1.drop()
df_tmp2.drop()
print("删除临时表成功")
print("process successfully!")
print("=====================")