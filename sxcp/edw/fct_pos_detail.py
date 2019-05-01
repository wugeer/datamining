# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import datetime
import re
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
# import sys

spark = SparkSession.builder.master("yarn").appName("fct_pos_detail").enableHiveSupport().getOrCreate()
# sc = SparkContext(appName="PythonSQL", master="local[*]")
# sqlContext = SQLContext(sc)
# hc = HiveContext(sc)

sql_sal1 = """
select * from rbu_sxcp_ods_dev.ods_sal1
"""

sql_oshp = """
select docentry,shpCode from rbu_sxcp_ods_dev.ods_oshp
"""

sql_oitm = """
select docentry,ItemCode,MsrUnit from rbu_sxcp_ods_dev.ods_oitm
"""

df_ods_sal1 = spark.sql(sql_sal1)
df_ods_oshp = spark.sql(sql_oshp)
df_ods_oitm = spark.sql(sql_oitm)


df_ods_oshp.registerTempTable("tmp_ods_oshp_spark")
df_ods_sal1.registerTempTable("tmp_ods_sal1_spark")
df_ods_oitm.registerTempTable("tmp_ods_oitm_spark")


def get_hour(item):
    if item and len(str(item)) > 13:
        return int(str(item)[11:13]) if int(str(item)[11:13]) > 2 else int(str(item)[11:13])+24


# 获取周几
def get_week(item):
    date1 = datetime.datetime.strptime(item[0:10], "%Y-%m-%d")
    date2 = datetime.datetime.strptime('2010-01-03', "%Y-%m-%d")
    num = (date1 - date2).days
    return num % 7


# 获取销售日期
def get_sale_day(item):
    if int(str(item)[11:13]) < 3:
        item = datetime.datetime.strptime(str(item)[:10], "%Y-%m-%d") + datetime.timedelta(days=-1)
    return str(item)[:10]


get_sale_day = spark.udf.register("get_sale_day", get_sale_day)
get_week = spark.udf.register("get_week", get_week)
get_hour = spark.udf.register("get_hour", get_hour)

print("starting..........")
tmp1_sql = """
select 
a.DocNum doc_no,
b.shpCode shop_code,
c.ItemCode product_code,
a.Qty1 qty,
c.MsrUnit unit,
a.MarkedPrice price_origin,
a.Price price,  
a.LineTotal amt,
get_sale_day(a.exitdate) sale_date,
a.exitDate sale_time,
a.Memo remark,
get_week(a.exitDate) sale_weekday, 
get_hour(a.exitDate) sale_hour
from tmp_ods_sal1_spark a
left join tmp_ods_oshp_spark b on a.shpentry=b.docentry
left join tmp_ods_oitm_spark c on a.itementry=c.docentry
where b.shpCode is not null and c.ItemCode is not null and a.exitDate>'2018-12-14 03:48:43'
"""
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1_spark")

tmp2_sql = """
select 
doc_no,
shop_code,
product_code,
sum(amt) amt
from tmp1_spark 
group by  doc_no,shop_code,product_code
"""
df_tmp2 = spark.sql(tmp2_sql)
df_tmp2.createOrReplaceTempView("tmp2_spark")

insert_sql = """
insert into table rbu_sxcp_edw_dev.fct_pos_detail
(select 
a.doc_no,
b.amt,
NULL,
NULL,
a.shop_code,
a.product_code,
a.qty,
a.price_origin,
a.price,
a.amt,
a.unit,
a.sale_date,
a.sale_time,
a.sale_hour,
a.sale_weekday,
a.remark,
current_timestamp()
from tmp1_spark a
left join tmp2_spark b on b.doc_no=a.doc_no and b.shop_code=a.shop_code and b.product_code=a.product_code)
"""
spark.sql(insert_sql)
print("插入数据成功")
df_tmp2.drop()
df_tmp1.drop()
df_ods_sal1.drop()
df_ods_oshp.drop()
df_ods_oitm.drop()
print("删除临时表成功")

print("process successfully!")
print("=====================")

