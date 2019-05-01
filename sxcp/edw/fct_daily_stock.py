from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession

spark = SparkSession.builder.master("yarn").appName("fct_daily_stock").getOrCreate()

sql_tmp1 = """
select 
row_number() over(partition by b.shpcode,c.itemcode,a.CheckDate order by a.docentry) doc_id,
a.docentry docentry,
b.shpcode shop_code,
c.itemcode product_code,
c.MsrUnit unit,
a.Qty1 qty,
a.CheckDate check_date
from rbu_sxcp_ods_dev.ods_sck1 a
left join rbu_sxcp_ods_dev.ods_oshp b on a.shpentry=b.docentry
left join  rbu_sxcp_ods_dev.ods_oitm c on a.itementry=c.docentry
where b.shpcode is not null and c.itemcode is not null and CheckDate>'2018-12-14 03:50:36'
"""
df_tmp1 = spark.sql(sql_tmp1)
df_tmp1.createOrReplaceTempView("tmp1_spark")
print("生成临时表tmp1成功")

insert_sql = """
insert into table rbu_sxcp_edw_dev.fct_daily_stock
(select 
shop_code,
product_code,
unit,
qty,
check_date,
current_timestamp()
from tmp1_spark 
where doc_id=1)
"""
spark.sql(insert_sql)
print("插入数据成功")
df_tmp1.drop()
print("删除临时表成功")

print("process successfully!")
print("=====================")