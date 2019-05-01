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
a.measure_unit,
a.min_qty,
a.min_display_qty,
a.price_sale,
a.price_wholesale,
a.optimal_purchase,
c.safety_stock_qty,
a.final_purchase,
c.min_package_qty,
a.create_time,
e.rank,
e.turnover,
d.actual_end_qty,
d.calculate_end_qty,
d.check_status
from order a
left join rbu_sxcp_edw_dev.dim_store b on a.shop_code=b.store_code
left join rbu_sxcp_edw_dev.dim_product c on a.product_code=c.product_code
left join rbu_sxcp_edw_dev.check_daily_stock d on date_sub(a.bill_date,1)=d.check_date and a.shop_code=d.store_code and a.product_code=d.product_code
left join rbu_sxcp_rst_dev.seven_days_turnover_ranking e on a.shop_code=e.store_code and a.product_code=e.product_code and date_sub(a.bill_date,1)=e.sale_date
"""
df_res = spark.sql(sql_res)
df_res = df_res.dropDuplicates()
df_res.createOrReplaceTempView("res")

insert_rst_product_bill = """
insert overwrite table rbu_sxcp_rst_dev.rst_product_bill
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
measure_unit,
min_qty,
min_display_qty,
price_sale,
price_wholesale,
optimal_purchase,
NULL planner_purchase,
NULL planner_director_purchase,
safety_stock_qty,
final_purchase,
min_package_qty,
NULL modify_reason,
NULL modify_purchase,
NULL modify_reason_type,
NULL planner_modify_reason,
NULL planner_modify_reason_type,
NULL planner_director_modify_reason,
NULL planner_director_modify_reason_type,
NULL max_modify,
rank,
turnover,
actual_end_qty,
calculate_end_qty,
coalesce(check_status,0),
create_time,
current_timestamp(),
NULL remark,
NULL extra1,
NULL extra2
from res)
"""
print("开始插入rst_product_bill")
spark.sql(insert_rst_product_bill)
print("插入rst_product_bill成功")

print("process successfully!")
print("=====================")
