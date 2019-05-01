from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from datetime import datetime, date, timedelta
import time

spark = SparkSession.builder.master("yarn").appName("fct_daily_stock").getOrCreate()
item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")


print("starting.......")
print("开始插入%s 日末库存数据"%item)
sql_tmp1 = """
select 
a.docentry docentry,
b.shpcode shop_code,
c.itemcode product_code,
c.MsrUnit unit,
a.Qty1 qty,
to_date(a.CheckDate) check_date,
row_number() over(partition by b.shpcode,c.itemcode order by a.docentry desc) date_id
from rbu_sxcp_ods_dev.ods_sck1 a
left join rbu_sxcp_ods_dev.ods_oshp b on a.shpentry=b.docentry
left join  rbu_sxcp_ods_dev.ods_oitm c on a.itementry=c.docentry
where b.shpcode is not null and c.itemcode is not null and to_date(CheckDate)='{0}' and b.shpcode in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
""".format(item)
df_tmp1 = spark.sql(sql_tmp1)
df_tmp1 = df_tmp1.dropDuplicates()
df_tmp1.createOrReplaceTempView("tmp1_spark")
# print("生成临时表tmp1成功")
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')

insert_sql = """
insert overwrite table rbu_sxcp_edw_dev.fct_daily_stock partition(year_month)
(select 
shop_code,
product_code,
unit,
qty,
check_date,
current_timestamp() etl_time,
substring(check_date,1,7) year_month 
from tmp1_spark
where date_id=1
union all
select 
store_code, 
product_code, 
unit,
qty, 
check_date, 
etl_time,  
year_month
from rbu_sxcp_edw_dev.fct_daily_stock
where check_date<'{0}' 
    and check_date>=concat(substring('{0}',1,7),'-01'))
DISTRIBUTE BY rand()
""".format(item)
print("开始插入数据")
start = time.time()
spark.sql(insert_sql)
end = time.time()
print('Running time: %s Seconds' % (end - start))
print("插入数据成功")
df_tmp1.drop()
print("删除临时表成功")

print("process successfully!")
print("=====================")