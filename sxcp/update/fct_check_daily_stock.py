from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from datetime import datetime, date, timedelta
import time

spark = SparkSession.builder.master("yarn").appName("fct_check_daily_stock").getOrCreate()
print("starting.......")
item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
print("开始生成%s fct_check_daily_stock"%item)
# 取io与库存并集
tmp1_sql = """
select 
    t1.store_code,
    t1.product_code,
    t1.io_date check_date
from rbu_sxcp_edw_dev.fct_io t1 
where t1.io_date='{0}'
group by 
    t1.store_code,
    t1.product_code,
    t1.io_date
union 
select 
    t2.store_code,
    t2.product_code,
    t2.check_date
from rbu_sxcp_edw_dev.fct_daily_stock t2 
where t2.check_date='{0}'
group by 
    t2.store_code,
    t2.product_code,
    t2.check_date
""".format(item)
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1")

#合计出库数量
tmp2_sql = """
select 
    store_code,
    product_code,
    sum(qty) out_qty,
    io_date
from rbu_sxcp_edw_dev.fct_io
where io_date='{0}'
    and io_type in ('调拨出库', '出库', '报损')
group by 
    store_code,
    product_code,
    io_date
""".format(item)
df_tmp2 = spark.sql(tmp2_sql)
df_tmp2.createOrReplaceTempView("tmp2")
# spark.catalog.cacheTable("tmp2")

#合计入库数量
tmp3_sql = """
select 
    store_code,
    product_code,
    sum(qty) in_qty,
    io_date
from rbu_sxcp_edw_dev.fct_io
where io_date='{0}'
    and io_type in ('调拨入库', '入库')
group by 
    store_code,
    product_code,
    io_date
""".format(item)
df_tmp3 = spark.sql(tmp3_sql)
df_tmp3.createOrReplaceTempView("tmp3")


#计算库存
tmp4_sql="""
select 
    t1.store_code,
    t1.product_code,
    t1.check_date,
    coalesce(t4.actual_end_qty,t4.calculate_end_qty,0)+coalesce(t3.in_qty,0)-coalesce(t2.out_qty,0) calculate_end_qty
    t5.qty actual_end_qty,
    case 
        when coalesce(t4.actual_end_qty,t4.calculate_end_qty,0)+coalesce(t3.in_qty,0)-coalesce(t2.out_qty,0)=coalesce(t5.qty,0) then 1
        else 0
    end check_status,
    current_timestamp etl_time,
    substring(t1.check_date,1,7) year_month
from tmp1 t1 
left join tmp2 t2
    on t1.store_code=t2.store_code
    and t1.product_code=t2.product_code
    and t1.check_date=t2.io_date
left join tmp3 t3
    on t1.store_code=t3.store_code
    and t1.product_code=t3.product_code
    and t1.check_date=t3.io_date
left join rbu_sxcp_edw_dev.fct_check_daily_stock t4 
    on t1.store_code=t4.store_code
    and t1.product_code=t4.product_code
    and t1.check_date=t4.check_date+1
left join rbu_sxcp_edw_dev.fct_daily_stock t5 
    on t1.store_code=t5.store_code
    and t1.product_code=t5.product_code
    and t1.check_date=t5.check_date
"""
df_tmp4 = spark.sql(tmp4_sql)
df_tmp4.createOrReplaceTempView("tmp4")

#插入数据
insert_sql = """
insert overwrite table rbu_sxcp_edw_dev.fct_check_daily_stock partition(year_month)
(
select 
    store_code,
    product_code,
    check_date,
    calculate_end_qty,
    actual_end_qty,
    check_status,
    etl_time,
    year_month
from rbu_sxcp_edw_dev.fct_check_daily_stock
where check_date<'{0}'
    and check_date>=concat(substring('{0}',1,7),'-01')
union all
select 
    store_code,
    product_code,
    check_date,
    calculate_end_qty,
    actual_end_qty,
    check_status,
    etl_time,
    year_month
from tmp4)
"""
print("开始插入数据")
start = time.time()
spark.sql(insert_sql)
end = time.time()
print('Running time: %s Seconds' % (end - start))
print("插入数据成功")


df_tmp1.drop()
df_tmp2.drop()
df_tmp3.drop()
df_tmp4.drop()


# spark.catalog.uncacheTable("loss")
# spark.catalog.uncacheTable("out_io")
# spark.catalog.uncacheTable("in_io")
# spark.catalog.uncacheTable("tmp7")
print("process successfully!")
print("=====================")