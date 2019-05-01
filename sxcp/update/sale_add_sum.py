# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import re
import time
import pandas as pd
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
from datetime import datetime, date, timedelta
spark = SparkSession.builder.master("yarn").appName("mid_sale_add_sum").enableHiveSupport().getOrCreate()


print("starting..........")
item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
print("开始生成%s 总销补数据"%item)

sql_store_product_on_sale = """
select store_code,product_code from rbu_sxcp_edw_dev.fct_on_sale_history
where sale_date='{0}' and store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
""".format(item)
df_store_product_on_sale = spark.sql(sql_store_product_on_sale)
df_store_product_on_sale.createOrReplaceTempView("tmp_store_product_on_sale_spark")

print("获取在售商品表信息成功")
sql_fct_pos_detail = """
select * from rbu_sxcp_edw_dev.fct_pos_detail 
where store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
"""
df_fct_pos_detail = spark.sql(sql_fct_pos_detail)
df_fct_pos_detail.createOrReplaceTempView("tmp_fct_pos_detail_spark")

sql_fct_daily_stock = """
select * from rbu_sxcp_edw_dev.fct_daily_stock 
where store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
"""
df_fct_daily_stock = spark.sql(sql_fct_daily_stock)
df_fct_daily_stock.createOrReplaceTempView("tmp_fct_daily_stock_spark")

sql_store_category = """
select store_code, date,case date_case from rbu_sxcp_edw_ai_dev.store_category
where store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
"""
df_store_category = spark.sql(sql_store_category)
df_store_category.createOrReplaceTempView("store_category_spark")


def generate_series(start, end, step):
    res = [[start], ]
    while res[-1][0] < end:
        res.append([str(datetime.strptime(res[-1][0], "%Y-%m-%d") + timedelta(days=step))[:10]])
    return res


time_list = generate_series('2016-07-01', item, 1)
ll3 = pd.DataFrame(time_list, columns=['sale_date'])
cc = ll3.values.tolist()
dd = list(ll3.columns)

df_time_list = spark.createDataFrame(cc, dd)
df_time_list.createOrReplaceTempView("tmp_time_list")

tmp0_sql = """
select distinct store_code,product_code,b.sale_date from tmp_store_product_on_sale_spark
cross join tmp_time_list b
""".format(item)
df_tmp0 = spark.sql(tmp0_sql)
df_tmp0.createOrReplaceTempView("tmp0_spark")

tmp1_sql = """
select 
store_code,
product_code,
sale_date,
max(sale_hour) max_hour
from tmp_fct_pos_detail_spark
where sale_date>='2016-07-01'
group by store_code,product_code,sale_date
"""
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1_spark")

tmp2_sql = """
select 
t0.store_code,
t0.product_code,
t0.sale_date,
t10.date_case,
t4.sale_time,
t4.sale_hour,
t4.qty,
t1.max_hour,
t3.qty end_qty
from tmp0_spark t0
left join tmp1_spark t1 on t0.store_code=t1.store_code and t0.product_code=t1.product_code and t0.sale_date=t1.sale_date 
left join tmp_fct_daily_stock_spark t3 on t0.store_code=t3.store_code and t0.product_code=t3.product_code and t0.sale_date=t3.check_date
left join tmp_fct_pos_detail_spark t4 on t0.store_code=t4.store_code and t0.product_code=t4.product_code and t0.sale_date=t4.sale_date 
left join store_category_spark t10 on t10.store_code=t0.store_code and t10.date=t0.sale_date
"""
df_tmp2 = spark.sql(tmp2_sql)
df_tmp2.createOrReplaceTempView("tmp2_spark")

tmp3_sql = """
select 
store_code,
product_code,
sale_date,
date_case,
coalesce(max_hour,3) max_hour,
coalesce(sum(case when sale_hour>=3 then qty end),0) sum_3_sale,
coalesce(sum(case when sale_hour>=4 then qty end),0) sum_4_sale,
coalesce(sum(case when sale_hour>=5 then qty end),0) sum_5_sale,
coalesce(sum(case when sale_hour>=6 then qty end),0) sum_6_sale,
coalesce(sum(case when sale_hour>=7 then qty end),0) sum_7_sale,
coalesce(sum(case when sale_hour>=8 then qty end),0) sum_8_sale,
coalesce(sum(case when sale_hour>=9 then qty end),0) sum_9_sale,
coalesce(sum(case when sale_hour>=10 then qty end),0) sum_10_sale,
coalesce(sum(case when sale_hour>=11 then qty end),0) sum_11_sale,
coalesce(sum(case when sale_hour>=12 then qty end),0) sum_12_sale,
coalesce(sum(case when sale_hour>=13 then qty end),0) sum_13_sale,
coalesce(sum(case when sale_hour>=14 then qty end),0) sum_14_sale,
coalesce(sum(case when sale_hour>=15 then qty end),0) sum_15_sale,
coalesce(sum(case when sale_hour>=16 then qty end),0) sum_16_sale,
coalesce(sum(case when sale_hour>=17 then qty end),0) sum_17_sale,
coalesce(sum(case when sale_hour>=18 then qty end),0) sum_18_sale,
coalesce(sum(case when sale_hour>=19 then qty end),0) sum_19_sale,
coalesce(sum(case when sale_hour>=20 then qty end),0) sum_20_sale,
coalesce(sum(case when sale_hour>=21 then qty end),0) sum_21_sale,
coalesce(sum(case when sale_hour>=22 then qty end),0) sum_22_sale,
coalesce(sum(case when sale_hour>=23 then qty end),0) sum_23_sale,
coalesce(sum(case when sale_hour>=24 then qty end),0) sum_0_sale,
coalesce(sum(case when sale_hour>=25 then qty end),0) sum_1_sale,
coalesce(sum(case when sale_hour>=26 then qty end),0) sum_2_sale,
coalesce(end_qty,0) end_qty
from tmp2_spark
group by 
store_code,
product_code,
sale_date,
date_case,
max_hour,
end_qty
"""
df_tmp3 = spark.sql(tmp3_sql)
df_tmp3.createOrReplaceTempView("tmp3_spark")
#print("tmp3")
#df_tmp3.show()

tmp4_sql = """
select 
store_code,
product_code,
sale_date,
lead(sale_date,1,'2016-06-30') over(partition by store_code,product_code order by sale_date desc) before_date
from tmp3_spark
where max_hour<>3
"""
df_tmp4 = spark.sql(tmp4_sql)
df_tmp4.createOrReplaceTempView("tmp4_spark")
#print("tmp4")
#df_tmp4.show()

tmp5_sql = """
select 
store_code,
product_code,
sale_date,
before_date
from tmp4_spark
where date_sub(sale_date, 8) > before_date
"""
df_tmp5 = spark.sql(tmp5_sql)
df_tmp5.createOrReplaceTempView("tmp5_spark")
#print("tmp5")
#df_tmp5.show()

sql_tmp7 = """
select
t1.store_code,
t1.product_code,
t1.sale_date,
t1.date_case,
t1.max_hour,
t1.sum_3_sale,
t1.sum_4_sale,
t1.sum_5_sale,
t1.sum_6_sale,
t1.sum_7_sale,
t1.sum_8_sale,
t1.sum_9_sale,
t1.sum_10_sale,
t1.sum_11_sale,
t1.sum_12_sale,
t1.sum_13_sale,
t1.sum_14_sale,
t1.sum_15_sale,
t1.sum_16_sale,
t1.sum_17_sale,
t1.sum_18_sale,
t1.sum_19_sale,
t1.sum_20_sale,
t1.sum_21_sale,
t1.sum_22_sale,
t1.sum_23_sale,
t1.sum_0_sale,
t1.sum_1_sale,
t1.sum_2_sale,
t1.end_qty,
t2.store_code test
from tmp3_spark t1 
left join tmp5_spark t2 on t1.store_code=t2.store_code and t1.product_code=t2.product_code and t1.sale_date<t2.sale_date and t1.sale_date>t2.before_date
where t2.store_code is null
"""
df_tmp7 = spark.sql(sql_tmp7)
df_tmp7 = df_tmp7.dropDuplicates()
df_tmp7.createOrReplaceTempView("tmp7_spark")
#print("最终要插入的数据")
#df_tmp7.show()

spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')

insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_add_sum partition(year_month)
(
select 
    store_code,
    product_code,
    date_case,
    sale_date,
    max_hour,
    sum_3_sale,
    sum_4_sale,
    sum_5_sale,
    sum_6_sale,
    sum_7_sale,
    sum_8_sale,
    sum_9_sale,
    sum_10_sale,
    sum_11_sale,
    sum_12_sale,
    sum_13_sale,
    sum_14_sale,
    sum_15_sale,
    sum_16_sale,
    sum_17_sale,
    sum_18_sale,
    sum_19_sale,
    sum_20_sale,
    sum_21_sale,
    sum_22_sale,
    sum_23_sale,
    sum_0_sale,
    sum_1_sale,
    sum_2_sale,
    end_qty,
    etl_time,
    year_month
from rbu_sxcp_edw_ai_dev.sale_add_sum
where sale_date<'{0}' and sale_date>=concat(substring('{0}',1,7),'-01')
union all
select
    t1.store_code,
    t1.product_code,
    t1.date_case,
    t1.sale_date,
    t1.max_hour,
    t1.sum_3_sale,
    t1.sum_4_sale,
    t1.sum_5_sale,
    t1.sum_6_sale,
    t1.sum_7_sale,
    t1.sum_8_sale,
    t1.sum_9_sale,
    t1.sum_10_sale,
    t1.sum_11_sale,
    t1.sum_12_sale,
    t1.sum_13_sale,
    t1.sum_14_sale,
    t1.sum_15_sale,
    t1.sum_16_sale,
    t1.sum_17_sale,
    t1.sum_18_sale,
    t1.sum_19_sale,
    t1.sum_20_sale,
    t1.sum_21_sale,
    t1.sum_22_sale,
    t1.sum_23_sale,
    t1.sum_0_sale,
    t1.sum_1_sale,
    t1.sum_2_sale,
    t1.end_qty,
    current_timestamp() etl_time,
    substring(t1.sale_date,1,7) year_month
from tmp7_spark t1)
""".format(item)
print("开始插入数据")
start = time.time()
spark.sql(insert_sql)
end = time.time()
print('Running time: %s Seconds' % (end - start))
print("插入数据成功")


print("process successfully!")
print("=====================")
