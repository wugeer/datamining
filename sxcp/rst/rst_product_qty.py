# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession

spark = SparkSession.builder.master("yarn").appName("rst_product_bill").enableHiveSupport().getOrCreate()

print("start-----------------------------")

sql_order = """
SELECT
shop_code, bill_date, arrive_date, product_code, measure_unit, min_qty, min_display_qty, price_sale, price_wholesale, d0_end_qty, d1_in_qty, d2_in_qty, optimal_purchase, final_purchase, d3_sale_predict, d1_loss, d2_loss, d3_loss, d1_end_qty, d2_end_qty, d3_end_qty, d2_sale_predict, d1_sale_predict, create_time
FROM rbu_sxcp_edw_ai_dev.product_bill_order
"""
df_order = spark.sql(sql_order)
df_order = df_order.dropDuplicates()
df_order.createOrReplaceTempView("order")

sql_res = """
select 
a.shop_code,
b.store_name,
a.bill_date,
a.arrive_date,
c.large_class,
a.product_code,
c.product_name,
a.d0_end_qty,
a.d1_end_qty,
a.d1_sale_predict,
a.d1_loss,
a.d1_in_qty,
a.d2_in_qty,
a.d1_end_qty,
a.d2_end_qty,
a.d2_sale_predict,
a.d2_loss,
a.d3_end_qty,
a.d2_end_qty,
a.optimal_purchase d3_in_qty,
a.d3_sale_predict,
a.d3_loss,
create_time
from order a
left join rbu_sxcp_edw_dev.dim_store b on a.shop_code=b.store_code
left join rbu_sxcp_edw_dev.dim_product c on a.product_code=c.product_code
"""
df_res = spark.sql(sql_res)
df_res = df_res.dropDuplicates()
df_res.createOrReplaceTempView("res")

insert_product_qty = """
insert overwrite table rbu_sxcp_rst_dev.rst_product_qty_1
(select 
hash(shop_code),
hash(product_code),
shop_code,
store_name,
bill_date,
arrive_date,
large_class,
product_code,
product_name,
d0_end_qty,
d1_end_qty,
d1_sale_predict,
d1_loss,
d1_in_qty,
d2_in_qty,
d1_end_qty,
d2_end_qty,
d2_sale_predict,
d2_loss,
d3_end_qty,
d2_end_qty,
d3_in_qty,
d3_sale_predict,
d3_loss,
NULL remark,
NULL extra1,
NULL extra2,
create_time,
current_timestamp()
from res)
"""

print("开始插入rst_product_qty")
spark.sql(insert_product_qty)
print("插入成功rst_poroduct_qty")
#df_dim_store_product.drop()
#df_dim_store.drop()
#print("删除临时表成功")
#
print("process successfully!")
print("=====================")
