from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, UDFRegistration, SparkSession
from pyspark.sql.functions import udf
from datetime import datetime, date, timedelta
import time

spark = SparkSession.builder.master("yarn").appName("mid_turnover_feat").enableHiveSupport().getOrCreate()
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')

print("starting..........")
item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")

tmp0_sql = """
select distinct store_code 
from rbu_sxcp_edw_dev.fct_on_sale_history
where sale_date='{0}' and store_code in  ('st_code_0585', 'st_code_0292', 'st_code_0659') 
""".format(item)
df_tmp0 = spark.sql(tmp0_sql)
df_tmp0.createOrReplaceTempView("tmp0")

tmp1_sql = """
select 
t1.store_code,
t1.date sale_date,
t2.sale_hour,
t2.qty*coalesce(t3.sale_price,0) amt
from rbu_sxcp_edw_ai_dev.store_category  t1
inner join tmp0 t0 on t1.store_code=t0.store_code
left join rbu_sxcp_edw_dev.fct_pos_detail t2 
    on t1.store_code=t2.store_code 
    and t1.date=t2.sale_date
left join rbu_sxcp_edw_dev.dim_store_product t3 
    on t2.store_code=t3.store_code 
    and t2.product_code=t3.product_code 
where t1.date='{0}'
""".format(item)
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1")

tmp2_sql = """
select 
store_code,
sale_date,
sale_hour,
sum(coalesce(amt,0)) turnover
from tmp1
group by store_code,sale_date,sale_hour
"""
df_tmp2 = spark.sql(tmp2_sql)
df_tmp2.createOrReplaceTempView("tmp2")
df_tmp1.drop()

tmp3_sql = """
select 
store_code,
sale_date,
coalesce(sum(case when sale_hour>=3 then turnover end),0) sum_3_turnover,
coalesce(sum(case when sale_hour>=4 then turnover end),0) sum_4_turnover,
coalesce(sum(case when sale_hour>=5 then turnover end),0) sum_5_turnover,
coalesce(sum(case when sale_hour>=6 then turnover end),0) sum_6_turnover,
coalesce(sum(case when sale_hour>=7 then turnover end),0) sum_7_turnover,
coalesce(sum(case when sale_hour>=8 then turnover end),0) sum_8_turnover,
coalesce(sum(case when sale_hour>=9 then turnover end),0) sum_9_turnover,
coalesce(sum(case when sale_hour>=10 then turnover end),0) sum_10_turnover,
coalesce(sum(case when sale_hour>=11 then turnover end),0) sum_11_turnover,
coalesce(sum(case when sale_hour>=12 then turnover end),0) sum_12_turnover,
coalesce(sum(case when sale_hour>=13 then turnover end),0) sum_13_turnover,
coalesce(sum(case when sale_hour>=14 then turnover end),0) sum_14_turnover,
coalesce(sum(case when sale_hour>=15 then turnover end),0) sum_15_turnover,
coalesce(sum(case when sale_hour>=16 then turnover end),0) sum_16_turnover,
coalesce(sum(case when sale_hour>=17 then turnover end),0) sum_17_turnover,
coalesce(sum(case when sale_hour>=18 then turnover end),0) sum_18_turnover,
coalesce(sum(case when sale_hour>=19 then turnover end),0) sum_19_turnover,
coalesce(sum(case when sale_hour>=20 then turnover end),0) sum_20_turnover,
coalesce(sum(case when sale_hour>=21 then turnover end),0) sum_21_turnover,
coalesce(sum(case when sale_hour>=22 then turnover end),0) sum_22_turnover,
coalesce(sum(case when sale_hour>=23 then turnover end),0) sum_23_turnover,
coalesce(sum(case when sale_hour>=24 then turnover end),0) sum_0_turnover,
coalesce(sum(case when sale_hour>=25 then turnover end),0) sum_1_turnover,
coalesce(sum(case when sale_hour>=26 then turnover end),0) sum_2_turnover
from tmp2
group by store_code,sale_date
"""
df_tmp3 = spark.sql(tmp3_sql)
df_tmp3.createOrReplaceTempView("tmp3")
df_tmp2.drop()

tmp4_sql = """
select 
store_code,
sale_date,
sum_3_turnover-sum_4_turnover time_3_turnover,
sum_4_turnover-sum_5_turnover time_4_turnover,
sum_5_turnover-sum_6_turnover time_5_turnover,
sum_6_turnover-sum_7_turnover time_6_turnover,
sum_7_turnover-sum_8_turnover time_7_turnover,
sum_8_turnover-sum_9_turnover time_8_turnover,
sum_9_turnover-sum_10_turnover time_9_turnover,
sum_10_turnover-sum_11_turnover time_10_turnover,
sum_11_turnover-sum_12_turnover time_11_turnover,
sum_12_turnover-sum_13_turnover time_12_turnover,
sum_13_turnover-sum_14_turnover time_13_turnover,
sum_14_turnover-sum_15_turnover time_14_turnover,
sum_15_turnover-sum_16_turnover time_15_turnover,
sum_16_turnover-sum_17_turnover time_16_turnover,
sum_17_turnover-sum_18_turnover time_17_turnover,
sum_18_turnover-sum_19_turnover time_18_turnover,
sum_19_turnover-sum_20_turnover time_19_turnover,
sum_20_turnover-sum_21_turnover time_20_turnover,
sum_21_turnover-sum_22_turnover time_21_turnover,
sum_22_turnover-sum_23_turnover time_22_turnover,
sum_23_turnover-sum_0_turnover time_23_turnover,
sum_0_turnover-sum_1_turnover time_0_turnover,
sum_1_turnover-sum_2_turnover time_1_turnover,
sum_2_turnover time_2_turnover
from tmp3
"""
df_tmp4 = spark.sql(tmp4_sql)
df_tmp4.createOrReplaceTempView("tmp4")
#spark.catalog.cacheTable("tmp_turnover_feat_sum_spark")
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
insert_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.turnover_feat_sum partition(year_month)
(select
store_code,
sale_date,
time_3_turnover,
time_4_turnover,
time_5_turnover,
time_6_turnover,
time_7_turnover,
time_8_turnover,
time_9_turnover,
time_10_turnover,
time_11_turnover,
time_12_turnover,
time_13_turnover,
time_14_turnover,
time_15_turnover,
time_16_turnover,
time_17_turnover,
time_18_turnover,
time_19_turnover,
time_20_turnover,
time_21_turnover,
time_22_turnover,
time_23_turnover,
time_0_turnover,
time_1_turnover,
time_2_turnover,
substring('{0}',1,7) year_month
from tmp4 
union all
select
* 
from rbu_sxcp_edw_ai_dev.turnover_feat_sum
where sale_date<'{0}' and sale_date>=concat(substring('{0}',1,7), '-01') 
""".format(item)
print("开始插入%s turnover_feat_sum数据"%item)
start = time.time()
spark.sql(tmp4_sql)
end = time.time()
print("消耗了%s 秒" % (end-start))

print("process successfully!")
print("=====================")
print("到此结束")
