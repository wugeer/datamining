from pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import time
from datetime import datetime, date, timedelta

spark = SparkSession.builder.master("yarn").appName("fct_sale_add").enableHiveSupport().getOrCreate()

print("starting..........")
item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
print("开始生成%s 销补数据"%item)
sql_fct_store_product_on_sale = """
select store_code,product_code,sale_date 
from rbu_sxcp_edw_dev.fct_on_sale_history 
where sale_date='{0}' and store_code in  ('st_code_0585', 'st_code_0292', 'st_code_0659') 
""".format(item)
df_fct_store_product_on_sale = spark.sql(sql_fct_store_product_on_sale)
df_fct_store_product_on_sale.createOrReplaceTempView("tmp_fct_store_product_on_sale")
#spark.catalog.cacheTable("tmp_fct_store_product_on_sale")

sql_pos_detail = """
select * from rbu_sxcp_edw_dev.fct_pos_detail
where sale_date='{0}' and store_code in  ('st_code_0585', 'st_code_0292', 'st_code_0659') 
""".format(item)
df_pos_detail = spark.sql(sql_pos_detail)
df_pos_detail.createOrReplaceTempView("tmp_pos_detail")
#spark.catalog.cacheTable("tmp_pos_detail")

sql_store_category = """
select store_code,date,case 
from rbu_sxcp_edw_ai_dev.store_category 
where date='{0}' and store_code in  ('st_code_0585', 'st_code_0292', 'st_code_0659') 
""".format(item)
df_store_category = spark.sql(sql_store_category)
df_store_category.createOrReplaceTempView("tmp_store_category")
#spark.catalog.cacheTable("tmp_store_category")

sql_mid_sale_add_avg = """
select * from rbu_sxcp_edw_ai_dev.sale_add_avg 
where sale_date='{0}'
""".format(item)
df_mid_sale_add_avg = spark.sql(sql_mid_sale_add_avg)
df_mid_sale_add_avg.createOrReplaceTempView("tmp_mid_sale_add_avg")
#spark.catalog.cacheTable("tmp_mid_sale_add_avg")
# 缓存三张表了
sql_ods_owsr = """
select * from rbu_sxcp_ods_dev.ods_owsr
"""
df_ods_owsr = spark.sql(sql_ods_owsr)
df_ods_owsr.createOrReplaceTempView("tmp_ods_owsr")

sql_ods_wsr1 = """
select * from rbu_sxcp_ods_dev.ods_wsr1
"""
df_ods_wsr1 = spark.sql(sql_ods_wsr1)
df_ods_wsr1.createOrReplaceTempView("tmp_ods_wsr1")

sql_ods_oshp = """
select * from rbu_sxcp_ods_dev.ods_oshp
"""
df_ods_oshp = spark.sql(sql_ods_oshp)
df_ods_oshp.createOrReplaceTempView("tmp_ods_oshp")

sql_ods_oitm = """
select * from rbu_sxcp_ods_dev.ods_oitm
"""
df_ods_oitm = spark.sql(sql_ods_oitm)
df_ods_oitm.createOrReplaceTempView("tmp_ods_oitm")

sql_ods_product_bill_other = """
select * from  rbu_sxcp_ods_dev.ods_product_bill_other
"""
df_ods_product_bill_other = spark.sql(sql_ods_product_bill_other)
df_ods_product_bill_other.createOrReplaceTempView("tmp_ods_product_bill_other")
#spark.catalog.cacheTable("tmp_ods_product_bill_other")

tmp3_sql = """
select 
c.Shpcode store_code,
d.itemcode product_code,
b.Qty1 qty,
to_date(b.enterdate) loss_date
from rbu_sxcp_ods_dev.ods_owsr a
left join rbu_sxcp_ods_dev.ods_wsr1 b on a.docentry=b.docentry and a.enterdate=b.enterdate and a.shpentry=b.shpentry
left join rbu_sxcp_ods_dev.ods_oshp c on b.shpentry=c.docentry
left join rbu_sxcp_ods_dev.ods_oitm d on b.itementry=d.docentry 
where a.reason='产品报废' and c.shpcode is not null and to_date(b.enterdate)='{0}' 
""".format(item)
df_tmp3 = spark.sql(tmp3_sql)
df_tmp3.createOrReplaceTempView("tmp3_spark")

tmp4_sql = """
select 
store_code,
product_code,
sum(qty) qty,
loss_date
from tmp3_spark
group by store_code,product_code,loss_date
"""
df_tmp4 = spark.sql(tmp4_sql)
df_tmp4.createOrReplaceTempView("tmp4_spark")
spark.catalog.cacheTable("tmp4_spark")

tmp5_sql = """
select 
shop_code store_code,
product_code,
sum(fin_qty) qty,
to_date(bill_date) check_date
from tmp_ods_product_bill_other
where goods_type='团购要货' and to_date(bill_date)='{0}' 
group by shop_code,product_code,bill_date
""".format(item)
df_tmp5 = spark.sql(tmp5_sql)
df_tmp5.createOrReplaceTempView("tmp5_spark")
#spark.catalog.cacheTable("tmp5_spark")

sql_fct_daily_stock = """
select store_code,product_code,to_date(check_date) check_date,qty 
from rbu_sxcp_edw_dev.fct_daily_stock 
where to_date(check_date)='{0}' and store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
""".format(item)
df_fct_daily_stock = spark.sql(sql_fct_daily_stock)
df_fct_daily_stock.createOrReplaceTempView("tmp_fct_daily_stock")
spark.catalog.cacheTable("tmp_fct_daily_stock")

sql_ods_product_bill_shop = """
select * from rbu_sxcp_ods_dev.ods_product_bill_shop
"""
df_ods_product_bill_shop = spark.sql(sql_ods_product_bill_shop)
df_ods_product_bill_shop.createOrReplaceTempView("tmp_ods_product_bill_shop")
#spark.catalog.cacheTable("tmp_ods_product_bill_shop")

print("开始%s"%item)
# 取当天在售商品
tmp0_sql = """
select 
store_code,
product_code,
sale_date check_date
from tmp_fct_store_product_on_sale
""".format(item)
df_tmp0 = spark.sql(tmp0_sql)
df_tmp0 = df_tmp0.dropDuplicates()
df_tmp0.createOrReplaceTempView("tmp0_spark")

tmp1_sql = """
select
store_code,
product_code,
sale_date check_date,
max(sale_hour) max_hour,
sum(qty) sale_qty
from tmp_pos_detail
where sale_date='{0}'
group by store_code,
product_code,
sale_date
""".format(item)
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1_spark")

tmp2_sql = """
select
t0.store_code,
t0.product_code,
t0.check_date,
coalesce(t1.max_hour,3) max_hour,
t1.sale_qty,
t2.case date_case
from tmp0_spark t0
left join tmp1_spark t1 on t0.store_code=t1.store_code and t0.product_code=t1.product_code and t0.check_date=t1.check_date
left join tmp_store_category t2 on t1.store_code=t2.store_code and t1.check_date=t2.date
where t0.store_code is not null and t0.product_code is not null and t0.check_date is not null and coalesce(t1.sale_qty,0)>=0
"""
df_tmp2 = spark.sql(tmp2_sql)
df_tmp2.createOrReplaceTempView("tmp2_spark")

tmp6_sql = """
select
t2.store_code,
t2.product_code,
t2.check_date,
t2.sale_qty,
case when max_hour=3 then avg_4_sale+avg_5_sale+avg_6_sale+avg_7_sale+avg_8_sale+avg_9_sale+avg_10_sale+avg_11_sale+avg_12_sale+avg_13_sale+avg_14_sale+avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale
    when max_hour=4 then avg_5_sale+avg_6_sale+avg_7_sale+avg_8_sale+avg_9_sale+avg_10_sale+avg_11_sale+avg_12_sale+avg_13_sale+avg_14_sale+avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale
    when max_hour=5 then avg_6_sale+avg_7_sale+avg_8_sale+avg_9_sale+avg_10_sale+avg_11_sale+avg_12_sale+avg_13_sale+avg_14_sale+avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale
    when max_hour=6 then avg_7_sale+avg_8_sale+avg_9_sale+avg_10_sale+avg_11_sale+avg_12_sale+avg_13_sale+avg_14_sale+avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale
    when max_hour=7 then avg_8_sale+avg_9_sale+avg_10_sale+avg_11_sale+avg_12_sale+avg_13_sale+avg_14_sale+avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=8 then avg_9_sale+avg_10_sale+avg_11_sale+avg_12_sale+avg_13_sale+avg_14_sale+avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=9 then avg_10_sale+avg_11_sale+avg_12_sale+avg_13_sale+avg_14_sale+avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=10 then avg_11_sale+avg_12_sale+avg_13_sale+avg_14_sale+avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=11 then avg_12_sale+avg_13_sale+avg_14_sale+avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=12 then avg_13_sale+avg_14_sale+avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=13 then avg_14_sale+avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=14 then avg_15_sale+avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=15 then avg_16_sale+avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=16 then avg_17_sale+avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=17 then avg_18_sale+avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=18 then avg_19_sale+avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=19 then avg_20_sale+avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=20 then avg_21_sale+avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=21 then avg_22_sale+avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale     
    when max_hour=22 then avg_23_sale+avg_0_sale+avg_1_sale+avg_2_sale   
    when max_hour=23 then avg_0_sale+avg_1_sale+avg_2_sale    
    when max_hour=24 then avg_1_sale+avg_2_sale    
    when max_hour=25 then avg_2_sale
    end sale_add
from tmp2_spark t2
left join tmp_mid_sale_add_avg t1 on t1.store_code=t2.store_code and t1.product_code=t2.product_code and t1.sale_date=t2.check_date
"""
df_tmp6 = spark.sql(tmp6_sql)
df_tmp6.createOrReplaceTempView("tmp6_spark")
sql_sale_add = """
SELECT store_code, product_code, check_date, 
sale_qty, sale_add_qty, loss_qty, sale_total_qty, group_buying_qty, end_qty, etl_time, year_month
FROM rbu_sxcp_edw_ai_dev.store_sale_add
"""
df_sale_add = spark.sql(sql_sale_add)
df_sale_add.createOrReplaceTempView("tmp_sale_add")
spark.catalog.cacheTable("tmp_sale_add")

tmp7_sql = """
select 
store_code,
product_code,
check_date,
sum(qty) end_qty
from tmp_fct_daily_stock
group by store_code,product_code,check_date
"""
df_tmp7 = spark.sql(tmp7_sql)
df_tmp7.createOrReplaceTempView("tmp7_spark")
sql_dis = """
select
row_number() over(partition by a.store_code,a.product_code,a.check_date order by a.sale_add desc) date_id,
a.store_code,
a.product_code,
a.sale_qty,
a.sale_add,
b.qty loss_qty,
coalesce(a.sale_qty,0)+coalesce(a.sale_add,0) sale_total_qty,
c.qty group_buying_qty,
d.end_qty,
current_timestamp() etl_time,
a.check_date
from tmp6_spark a
left join tmp4_spark b on a.store_code=b.store_code and a.product_code=b.product_code and b.loss_date=a.check_date
left join tmp5_spark c on c.store_code=a.store_code and c.product_code=a.product_code and c.check_date=a.check_date
left join tmp7_spark d on d.store_code=a.store_code and d.product_code=a.product_code and a.check_date=d.check_date
where coalesce(b.qty,0)>=0 and coalesce(c.qty,0)>=0 and coalesce(a.sale_add,0)>=0 and coalesce(d.end_qty,0)>=0
"""
df_dis = spark.sql(sql_dis)
df_dis.createOrReplaceTempView("tmp_dis")
sql_hh = """
select
store_code,
product_code,
sale_qty,
sale_add_qty,
loss_qty,
sale_total_qty,
group_buying_qty,
end_qty,
etl_time,
check_date
from tmp_sale_add
where check_date<'{0}'
union
select
store_code,
product_code,
sale_qty,
sale_add,
loss_qty,
sale_total_qty,
group_buying_qty,
end_qty,
etl_time,
check_date
from tmp_dis
where date_id=1
""".format(item)
df_res_1 = spark.sql(sql_hh)
df_res_1.createOrReplaceTempView("tmp_res_1")

spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.store_sale_add partition(year_month)
(select
store_code,
product_code,
check_date,
sale_qty,
sale_add_qty,
loss_qty,
sale_total_qty,
group_buying_qty,
coalesce(end_qty,0) end_qty,
etl_time,
substring(check_date,1,7) year_month
from tmp_res_1)
"""
print("开始插入数据")
start = time.time()
spark.sql(insert_sql)
end = time.time()
print("插入数据")
print('Running time: %s Seconds' % (end - start))

print("删除缓存表成功")
print("process successfully!")
print("=====================")
print("到此结束")
