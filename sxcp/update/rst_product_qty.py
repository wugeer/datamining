# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from datetime import datetime, date, timedelta
spark = SparkSession.builder.master("yarn").appName("rst_product_bill").enableHiveSupport().getOrCreate()

print("start-----------------------------")
item = date.today().strftime("%Y-%m-%d")
# sql_order = """
# SELECT * FROM rbu_sxcp_edw_ai_dev.product_bill_order
# """
# df_order = spark.sql(sql_order)
# df_order = df_order.dropDuplicates()
# df_order.createOrReplaceTempView("order")

sql_bill_order = """
SELECT * FROM rbu_sxcp_edw_ai_dev.product_bill_order_test
"""
df_bill_order = spark.sql(sql_bill_order)
#df_bill_order = df_loss_pre_end_qty.dropDuplicates()
df_bill_order.createOrReplaceTempView("bill_order")
df_bill_order.show()
# spark.sql("select * from rbu_sxcp_edw_ai_dev.store_sale_add").show()

sql_loss_pre_end_qty = """
SELECT product_code, arrive_qty, end_qty, loss_qty, sale_date, sale_predict, start_qty, shop_code, bill_date
FROM rbu_sxcp_edw_ai_dev.loss_pre_end_qty
"""
df_loss_pre_end_qty = spark.sql(sql_loss_pre_end_qty)
#df_loss_pre_end_qty = df_loss_pre_end_qty.dropDuplicates()
df_loss_pre_end_qty.createOrReplaceTempView("loss_pre_end_qty")
df_loss_pre_end_qty.show()

#item = '2019-01-14'
sql_final = """
select
a.shop_code,
e.store_name,
a.bill_date,
a.arrive_date,
f.large_class,
a.product_code,
f.product_name,
b.start_qty d1_start_qty,
b.end_qty d1_end_qty,
b.arrive_qty d1_in_qty,
b.sale_predict d1_sale_predict,
b.loss_qty d1_loss_qty,
c.start_qty d2_start_qty,
c.end_qty d2_end_qty,
c.arrive_qty d2_in_qty,
c.sale_predict d2_sale_predict,
c.loss_qty d2_loss_qty,
d.start_qty d3_start_qty,
d.end_qty d3_end_qty,
d.arrive_qty d3_in_qty,
d.sale_predict d3_sale_predict,
d.loss_qty d3_loss_qty,
current_date() create_time,
current_timestamp() etl_time
from bill_order a 
left join loss_pre_end_qty b on a.shop_code=b.shop_code and b.product_code=a.product_code and a.bill_date=b.bill_date and b.sale_date='{0}'
left join loss_pre_end_qty c on a.shop_code=c.shop_code and a.product_code=c.product_code and a.bill_date=c.bill_date and c.sale_date=date_add('{0}', 1)
left join loss_pre_end_qty d on a.shop_code=d.shop_code and a.product_code=d.product_code and a.bill_date=d.bill_date and d.sale_date=date_add('{0}', 2)
left join rbu_sxcp_edw_dev.dim_store e on a.shop_code=e.store_code
left join rbu_sxcp_edw_dev.dim_product f on a.product_code=f.product_code
where a.bill_date='{0}'
""".format(item)
print(sql_final)
df_final = spark.sql(sql_final)
df_final = df_final.dropDuplicates()
df_final.createOrReplaceTempView("final")
df_final.show()

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
d1_start_qty,
d1_end_qty,
d1_sale_predict,
d1_loss_qty,
d1_in_qty,
d2_in_qty,
d2_start_qty,
d2_end_qty,
d2_sale_predict,
d2_loss_qty,
d3_end_qty,
d3_start_qty,
d3_in_qty,
d3_sale_predict,
d3_loss_qty,
NULL remark,
NULL extra1,
NULL extra2,
create_time,
etl_time
from final)
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
