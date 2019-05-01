from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from datetime import datetime, date, timedelta
import time

spark = SparkSession.builder.master("yarn").appName("update_case").getOrCreate()
print("starting.......")
item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
print("开始生成%s update_case"%item)


spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')

tmp1_sql = """
SELECT 
    t1.store_code, 
    t1.product_code, 
    coalesce(t2.case,t1.date_case) date_case, 
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
    t1.etl_time, 
    t1.year_month
FROM rbu_sxcp_edw_ai_dev.sale_add_sum t1 
left join rbu_sxcp_edw_ai_dev.store_category t2 
    on t1.store_code=t2.store_code
    and t1.sale_date=t2.date
"""
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1")


sale_add_sum = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_add_sum partition(year_month) 
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
from tmp1
"""


tmp2_sql = """
select 
    t1.store_code,
    t1.sale_date,
    coalesce(t2.case,t1.date_case) date_case,
    t1.turnover,
    t1.pre,
    t1.temp,
    t1.recent_3,
    t1.recent_7,
    t1.etl_time, 
    t1.year_month
from rbu_sxcp_edw_ai_dev.turnover_feat t1 
left join rbu_sxcp_edw_ai_dev.store_category t2 
    on t1.store_code=t2.store_code
    and t1.sale_date=t2.date
"""
df_tmp2 = spark.sql(tmp2_sql)
df_tmp2.createOrReplaceTempView("tmp2")

turnover_feat = """
insert overwrite table rbu_sxcp_edw_ai_dev.turnover_feat partition(year_month)
select 
    store_code,
    sale_date,
    date_case,
    turnover,
    pre,
    temp,
    recent_3,
    recent_7,
    etl_time, 
    year_month
from tmp2
"""


tmp3_sql = """
select 
    t1.store_code, 
    t1.product_code, 
    t1.sale_date, 
    coalesce(t2.case,t1.date_case) date_case, 
    t1.turnover,
    t1.temp, 
    t1.qty, 
    t1.recent_3, 
    t1.recent_7, 
    t1.etl_time, 
    t1.year_month
from rbu_sxcp_edw_ai_dev.sale_feat t1
left join rbu_sxcp_edw_ai_dev.store_category t2 
    on t1.store_code=t2.store_code
    and t1.sale_date=t2.date
"""
df_tmp3 = spark.sql(tmp3_sql)
df_tmp3.createOrReplaceTempView("tmp3")

sale_feat = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_feat partition(year_month)
select 
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
from tmp3
"""


print("开始插入数据")
start = time.time()
spark.sql(sale_add_sum)
spark.sql(turnover_feat)
spark.sql(sale_feat)
end = time.time()
print('Running time: %s Seconds' % (end - start))
print("插入数据成功")


df_tmp1.drop()
df_tmp2.drop()
df_tmp3.drop()