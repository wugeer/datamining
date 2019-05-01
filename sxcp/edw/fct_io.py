# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import datetime
import re

# url = "jdbc:postgresql://47.98.135.132:2345/gpdb?user=gpadmin&password=bigdata2018"
spark = SparkSession.builder.master("yarn").appName("fct_io").getOrCreate()

sql_wor1 = """
select docentry,qty1,linetotal,duedate,itementry,shpentry from rbu_sxcp_ods_dev.ods_wor1
"""

sql_oshp = """
select shpcode,docentry from rbu_sxcp_ods_dev.ods_oshp
"""

sql_oitm = """
select docentry,itemcode,msrunit from rbu_sxcp_ods_dev.ods_oitm
"""

sql_wsl1 = """
select shpentry,itementry,Qty1,LineTotal,exitdate from rbu_sxcp_ods_dev.ods_wsl1
"""
sql_sal1 = """
select shpentry,itementry,Qty1,LineTotal,docnum,exitdate from rbu_sxcp_ods_dev.ods_sal1
"""

sql_str1 = """
select shpoutentry,shpinentry,itementry,qty1,LineTotal,exitdate from rbu_sxcp_ods_dev.ods_str1
"""

sql_owor = """
select docnum,doctype,docentry,duedate from rbu_sxcp_ods_dev.ods_owor
"""

df_ods_owor = spark.sql(sql_owor)
df_ods_wor1 = spark.sql(sql_wor1)
df_ods_oitm = spark.sql(sql_oitm)
df_ods_oshp = spark.sql(sql_oshp)
df_ods_wsl1 = spark.sql(sql_wsl1)
df_ods_sal1 = spark.sql(sql_sal1)
df_ods_str1 = spark.sql(sql_str1)

df_ods_oshp.createOrReplaceTempView("tmp_ods_oshp_spark")
df_ods_wor1.createOrReplaceTempView("tmp_ods_wor1_spark")
df_ods_owor.createOrReplaceTempView("tmp_ods_owor_spark")
df_ods_oitm.createOrReplaceTempView("tmp_ods_oitm_spark")
df_ods_wsl1.createOrReplaceTempView("tmp_ods_wsl1_spark")
df_ods_sal1.createOrReplaceTempView("tmp_ods_sal1_spark")
df_ods_str1.createOrReplaceTempView("tmp_ods_str1_spark")

insert1_sql = """
insert into table rbu_sxcp_edw_dev.fct_io
(select 
c.shpcode,
d.itemcode,
b.qty1,
b.linetotal,
d.msrunit,
a.docnum,
to_date(b.duedate),
b.duedate,
'订单',
a.doctype,
current_timestamp()
from tmp_ods_owor_spark a 
left join tmp_ods_wor1_spark b on a.docentry=b.docentry and a.duedate=b.duedate
left join tmp_ods_oshp_spark c on b.shpentry=c.docentry
left join tmp_ods_oitm_spark d on b.itementry=d.docentry
where c.shpcode is not null and d.itemcode is not null and b.duedate>'2018-12-12 18:51:25.0')
"""

insert2_sql = """
insert into table rbu_sxcp_edw_dev.fct_io
(select 
b.Shpcode,
c.Itemcode,
a.Qty1,
a.LineTotal,
c.msrunit,
NULL,
to_date(a.exitdate),
a.exitdate,
'入库',
'入库',   
current_timestamp()
from tmp_ods_wsl1_spark a
left join tmp_ods_oshp_spark b on a.shpentry=b.docentry
left join tmp_ods_oitm_spark c on a.itementry=c.docentry 
where b.Shpcode is not null and c.Itemcode is not null and a.exitdate>'2018-12-14 03:49:34')
"""

insert3_sql = """
insert into table rbu_sxcp_edw_dev.fct_io
(select 
b.Shpcode,
c.Itemcode,
a.Qty1,
a.LineTotal,
c.msrunit,
a.docnum,
to_date(a.exitdate),
a.exitdate,
'出库',
'出库',
current_timestamp()
from tmp_ods_sal1_spark a
left join tmp_ods_oshp_spark b on a.shpentry=b.docentry
left join tmp_ods_oitm_spark c on a.itementry=c.docentry 
where b.Shpcode is not null and c.Itemcode is not null and a.exitdate>'2018-12-14 03:48:43')
"""

insert4_sql = """
insert into table rbu_sxcp_edw_dev.fct_io
(select 
t2.shpcode,
t4.itemcode,
t1.qty1,
t1.LineTotal,
t4.msrunit,
NULL,
to_date(t1.exitdate),
t1.exitdate,
'调拨出库',
'调拨',
current_timestamp()
from tmp_ods_str1_spark t1 
left join tmp_ods_oshp_spark t2 on t1.shpoutentry=t2.docentry
left join tmp_ods_oitm_spark t4 on t1.itementry=t4.docentry
where t2.shpcode is not null and t4.itemcode is not null and t1.exitdate>'2018-12-13 00:00:00.0')
"""

insert5_sql = """
insert into table rbu_sxcp_edw_dev.fct_io
(select 
t2.shpcode,
t4.itemcode,
t1.qty1,
t1.LineTotal,
t4.msrunit,
NULL,
to_date(t1.exitdate),
t1.exitdate,
'调拨入库',
'调拨',
current_timestamp()
from tmp_ods_str1_spark t1 
left join tmp_ods_oshp_spark t2 on t1.shpinentry=t2.docentry
left join tmp_ods_oitm_spark t4 on t1.itementry=t4.docentry
where t2.shpcode is not null and t4.itemcode is not null and t1.exitdate>'2018-12-13 00:00:00.0')
"""
spark.sql(insert1_sql)
print("插入订单信息成功")
spark.sql(insert2_sql)
print("插入入库信息成功")
spark.sql(insert3_sql)
print("插入出库信息成功")
spark.sql(insert4_sql)
print("插入调拨入库信息成功")
spark.sql(insert5_sql)
print("插入调拨出库信息成功")

print("插入数据成功")
df_ods_oitm.drop()
df_ods_oshp.drop()
df_ods_owor.drop()
df_ods_sal1.drop()
df_ods_str1.drop()
df_ods_wor1.drop()
df_ods_wsl1.drop()
print("删除临时表成功")

print("process successfully!")
print("=====================")