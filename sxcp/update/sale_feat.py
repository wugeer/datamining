# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, UDFRegistration, SparkSession
from pyspark.sql.functions import udf
import re
import time
from datetime import datetime, date, timedelta

spark = SparkSession.builder.master("yarn").appName("sale_feat").enableHiveSupport().getOrCreate()
item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
# 得到每个月的一号
pre_item = '-'.join([item[:7], '01'])
print("开始生成%s sale_feat数据"%item)
print("starting..........")
sql_mid_turnover_feat = """
select 
store_code,
sale_date,
date_case,
turnover,
temp
from rbu_sxcp_edw_ai_dev.turnover_feat 
where sale_date='{0}'
""".format(item)
df_mid_turnover_feat = spark.sql(sql_mid_turnover_feat)
df_mid_turnover_feat.createOrReplaceTempView("tmp_mid_turnover_feat")
#spark.catalog.cacheTable("tmp_mid_turnover_feat")

sql_fct_store_sale_add = """
SELECT 
store_code, 
product_code, 
check_date, 
sale_total_qty
FROM rbu_sxcp_edw_ai_dev.store_sale_add
where check_date='{0}'
""".format(item)
df_fct_store_sale_add = spark.sql(sql_fct_store_sale_add)
df_fct_store_sale_add.createOrReplaceTempView("tmp_fct_store_sale_add")
#spark.catalog.cacheTable("tmp_fct_store_sale_add")


def check_date_case(item):
    res = item.split(',')
    for tmp in res:
        tmp = tmp.strip()
        if tmp == '' or not '0' <= tmp <= '6':
            return False
    return True


check_date_case = spark.udf.register("check_date_case", check_date_case)

tmp1_sql = """
select 
t1.store_code,
t2.product_code,
t1.sale_date,
t1.date_case,
coalesce(t1.turnover,0) turnover,
t1.temp,
t2.sale_total_qty qty,
substring(t1.sale_date,1,7) year_month
from tmp_mid_turnover_feat t1
left join tmp_fct_store_sale_add t2 on t1.store_code=t2.store_code and t1.sale_date=t2.check_date
where t1.sale_date='{0}' and t1.store_code is not null and t2.product_code is not null and t1.date_case is not null and Boolean(check_date_case(t1.date_case)) and coalesce(t1.turnover,0)>=0 and t2.sale_total_qty is not null and t2.sale_total_qty>=0
""".format(item)
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1_spark")

# 取出月初到现在的sale_feat数据
sql_mid_sale_feat_xhw_front = """
select a.store_code,
product_code,
a.sale_date,
b.case date_case,
turnover,
temp,
qty,
recent_3,
recent_7,
etl_time,
year_month
from rbu_sxcp_edw_ai_dev.sale_feat a
where sale_date<'{0}' and sale_date>=concat(substring('{0}',1,7), '-01')
""".format(item)
df_mid_sale_feat_xhw_front = spark.sql(sql_mid_sale_feat_xhw_front)
df_mid_sale_feat_xhw_front.createOrReplaceTempView("tmp_mid_sale_feat_xhw_front")

union1_sql = """
select
store_code,
product_code,
date_case,
turnover,
temp,
qty,
sale_date,
recent_3,
recent_7,
year_month
from tmp_mid_sale_feat_xhw_front
union
select 
store_code,
product_code,
date_case,
turnover,
temp,
qty,
sale_date,
NULL recent_3,
NULL recent_7,
year_month
from tmp1_spark
"""
print("插入sale_feat第一次")
df_union1 = spark.sql(union1_sql)
df_union1.createOrReplaceTempView("tmp_union1")

qitian_sql = """
select t3.store_code,
t4.product_code,
t3.sale_date,
t3.date_case,
case when t3.sale_date=date_add('{0}', 1) then t5.d1
    when  t3.sale_date=date_add('{0}', 2) then t5.d2
    when t3.sale_date=date_add('{0}', 3) then t5.d3
    when t3.sale_date=date_add('{0}', 4) then t5.d4
    when t3.sale_date=date_add('{0}', 5) then t5.d5
    when t3.sale_date=date_add('{0}', 6) then t5.d6
    when t3.sale_date=date_add('{0}', 7) then t5.d7
end turnover,
t3.temp
from rbu_sxcp_edw_ai_dev.turnover_feat t3
left join rbu_sxcp_edw_dev.fct_on_sale_history t4 on t3.store_code=t4.store_code and t4.sale_date='{0}'
left join rbu_sxcp_edw_ai_dev.turnover_predict t5 on t3.store_code=t5.store_code and t5.sale_date=date_add('{0}', 1)
where t3.sale_date>=date_add('{0}',1) and t3.sale_date<=date_add('{0}',7)
""".format(item)
df_tiqian = spark.sql(qitian_sql)
df_tiqian.createOrReplaceTempView("qitian")
#print("未来七天数据")
#df_tiqian.show()

tmp10_sql = """
select 
row_number() over(partition by store_code,product_code,date_case order by sale_date desc) date_id,
store_code,
product_code,
sale_date,
date_case,
qty
from tmp_union1
where sale_date<='{0}' and qty is not null
""".format(item)
df_tmp10 = spark.sql(tmp10_sql)
df_tmp10.createOrReplaceTempView("tmp10")

tmp11_sql = """
select 
store_code,
product_code,
sale_date,
date_case,
qty
from tmp10 
where date_id<=30
"""
df_tmp11 = spark.sql(tmp11_sql)
df_tmp11.createOrReplaceTempView("tmp11")

tmp12_sql = """
select 
percentile_approx(qty,0.25,9999)-1.5*(percentile_approx(qty,0.75,9999)-percentile_approx(qty,0.25,9999)) low,
percentile_approx(qty,0.75,9999)+1.5*(percentile_approx(qty,0.75,9999)-percentile_approx(qty,0.25,9999)) high,
store_code,
product_code,
date_case
from tmp11
group by store_code,product_code,date_case
"""
df_tmp12 = spark.sql(tmp12_sql)
df_tmp12.createOrReplaceTempView("tmp12")

tmp13_sql = """
select 
t6.store_code,
t6.product_code,
t6.sale_date,
t6.date_case,
t6.qty
from tmp11 t6
left join tmp12 t7 on t6.store_code=t7.store_code and t6.product_code=t7.product_code and t6.date_case=t7.date_case
where t6.qty>=t7.low and t6.qty<=t7.high
"""
df_tmp13 = spark.sql(tmp13_sql)
df_tmp13.createOrReplaceTempView("tmp13")

tmp14_sql = """
select 
row_number() over(partition by store_code,product_code,date_case order by sale_date desc) date_id,
store_code,
product_code,
sale_date,
date_case,
qty
from tmp13
"""
df_tmp14 = spark.sql(tmp14_sql)
df_tmp14.createOrReplaceTempView("tmp14")


tmp15_sql = """
select 
store_code,
product_code,
date_case,
avg(qty) recent_3
from tmp14
where date_id<=3
group by store_code,product_code,date_case
"""
df_tmp15 = spark.sql(tmp15_sql)
df_tmp15.createOrReplaceTempView("tmp15")

tmp16_sql = """
select 
store_code,
product_code,
date_case,
avg(qty) recent_7
from tmp14
where date_id<=7
group by store_code,product_code,date_case
"""
df_tmp16 = spark.sql(tmp16_sql)
df_tmp16.createOrReplaceTempView("tmp16")

tmp17_sql = """
select 
t11.store_code,
t11.product_code,
t11.sale_date,
t11.date_case,
t11.turnover,
t11.temp,
t12.recent_3,
t13.recent_7,
NULL qty
from qitian t11 
left join tmp15 t12 on t11.store_code=t12.store_code and t11.product_code=t12.product_code and t11.date_case=t12.date_case
left join tmp16 t13 on t11.store_code=t13.store_code and t11.product_code=t13.product_code and t11.date_case=t13.date_case
where t12.recent_3 is not null and t13.recent_7 is not null and  t12.recent_3>=0 and t13.recent_7>=0
"""
df_tmp17 = spark.sql(tmp17_sql)
df_tmp17.createOrReplaceTempView("tmp17")
#print("tmp17")
#df_tmp17.show()

tmp2_sql = """
select 
row_number() over(partition by store_code,product_code,date_case order by sale_date desc) date_id,
store_code,
product_code,
sale_date,
date_case,
qty
from tmp_union1
where sale_date<'{0}' and qty is not null
""".format(item)
df_tmp2 = spark.sql(tmp2_sql)
df_tmp2.createOrReplaceTempView("tmp2_spark")

tmp3_sql = """
select 
store_code,
product_code,
sale_date,
date_case,
qty
from tmp2_spark 
where date_id<=30
"""
df_tmp3 = spark.sql(tmp3_sql)
df_tmp3.createOrReplaceTempView("tmp3_spark")

print("计算上下限")
tmp4_1_sql = """
select 
percentile_approx(qty,0.25,9999)-1.5*(percentile_approx(qty,0.75,9999)-percentile_approx(qty,0.25,9999)) low,
percentile_approx(qty,0.75,9999)+1.5*(percentile_approx(qty,0.75,9999)-percentile_approx(qty,0.25,9999)) high,
store_code,
product_code,
date_case
from tmp3_spark
group by store_code,product_code,date_case
"""
df_tmp4_1 = spark.sql(tmp4_1_sql)
df_tmp4_1.createOrReplaceTempView("tmp4_1_spark")

tmp4_sql = """
select 
t6.store_code,
t6.product_code,
t6.sale_date,
t6.date_case,
t6.qty
from tmp3_spark t6
left join tmp4_1_spark t7 on t6.store_code=t7.store_code and t6.product_code=t7.product_code and t6.date_case=t7.date_case
where t6.qty>=t7.low and t6.qty<=t7.high
"""
print("剔除异常值")
df_tmp4 = spark.sql(tmp4_sql)
df_tmp4.createOrReplaceTempView("tmp4_spark")

tmp5_1_sql = """
select 
row_number() over(partition by store_code,product_code,date_case order by sale_date desc) date_id,
store_code,
product_code,
sale_date,
date_case,
qty
from tmp4_spark
where sale_date<'{0}'
""".format(item)
df_tmp5_1 = spark.sql(tmp5_1_sql)
df_tmp5_1.createOrReplaceTempView("tmp5_1_spark")

tmp5_sql = """
select 
store_code,
product_code,
date_case,
avg(qty) recent_3
from tmp5_1_spark
where date_id<=3
group by store_code,product_code,date_case
"""
df_tmp5 = spark.sql(tmp5_sql)
df_tmp5.createOrReplaceTempView("tmp5_spark")

tmp6_sql = """
select 
store_code,
product_code,
date_case,
avg(qty) recent_7
from tmp5_1_spark
where date_id<=7
group by store_code,product_code,date_case
"""
df_tmp6 = spark.sql(tmp6_sql)
df_tmp6.createOrReplaceTempView("tmp6_spark")

tmp7_sql = """
select 
t8.store_code,
t8.product_code,
t8.sale_date,
t8.date_case,
t8.turnover,
t8.temp,
t8.qty,
t9.recent_3,
t10.recent_7,
current_timestamp() etl_time,
t8.year_month
from tmp1_spark t8 
left join tmp5_spark t9 on t8.store_code=t9.store_code and t8.product_code=t9.product_code and t8.date_case=t9.date_case
left join tmp6_spark t10 on t8.store_code=t10.store_code and t8.product_code=t10.product_code and t8.date_case=t10.date_case 
where t9.recent_3 is not null and t9.recent_3>=0 and t10.recent_7 is not null and t10.recent_7>=0 
"""
print("合并当天数据")
df_tmp7 = spark.sql(tmp7_sql)
df_tmp7.createOrReplaceTempView("tmp7_spark")

tmp8_sql = """
select 
t1.store_code,
t1.product_code,
t1.sale_date,
t1.date_case,
t1.turnover,
t1.temp,
t1.qty,
t1.recent_3,
t1.recent_7,
t1.etl_time,
t1.year_month
from tmp7_spark t1
left join tmp_fct_store_sale_add t2 on t1.store_code=t2.store_code and t1.product_code=t2.product_code
where  t2.store_code is not null
""".format(item)
print("删除mid_sale_feat中七日没有销售或库存的商品")
df_tmp8 = spark.sql(tmp8_sql)
df_tmp8 = df_tmp8.dropDuplicates()
df_tmp8.createOrReplaceTempView("tmp8_spark")

union2_sql = """
select
store_code,
product_code,
date_case,
turnover,
temp,
qty,
sale_date,
recent_3,
recent_7,
etl_time,
year_month
from tmp_mid_sale_feat_xhw_front
union
select 
store_code,
product_code,
date_case,
turnover,
temp,
qty,
sale_date,
recent_3,
recent_7,
etl_time,
year_month
from tmp8_spark
union
select
store_code,
product_code,
date_case,
turnover,
temp,
qty,
sale_date,
recent_3,
recent_7,
current_timestamp(),
substring(sale_date,1,7) year_month
from tmp17
"""
df_union2 = spark.sql(union2_sql)
df_union2.createOrReplaceTempView("tmp_union2")

spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_feat partition(year_month)
(select 
store_code,
product_code,
sale_date,
date_case,
turnover,
temp,
qty,
recent_3,
recent_7,
etl_time,
year_month
from tmp_union2)
""".format(item)
print("插入数据")
start = time.time()
spark.sql(insert_sql)
end = time.time()
print('Running time: %s Seconds' % (end - start))
print("插入数据成功")


print("process successfully!")
print("=====================")

