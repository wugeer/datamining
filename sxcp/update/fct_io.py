# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
import time
import re
from datetime import datetime, date, timedelta
# url = "jdbc:postgresql://47.98.135.132:2345/gpdb?user=gpadmin&password=bigdata2018"
spark = SparkSession.builder.master("yarn").appName("fct_io").getOrCreate()

item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
print("starting.......")
print("开始插入%s fct_io数据"%item)
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
spark.sql("set hive.exec.reducers.bytes.per.reducer=1024000000")
insert1_sql = """
insert overwrite table rbu_sxcp_edw_dev.fct_io  partition(year_month)
(select
*
from rbu_sxcp_edw_dev.fct_io
where io_date<'{0}' and io_date>=concat(substring('{0}',1,7),'-01')
union all
select 
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
current_timestamp() etl_time,
substring(b.duedate,1,7) year_month
from rbu_sxcp_ods_dev.ods_owor a 
left join rbu_sxcp_ods_dev.ods_wor1 b on a.docentry=b.docentry and a.duedate=b.duedate
left join rbu_sxcp_ods_dev.ods_oshp c on b.shpentry=c.docentry
left join rbu_sxcp_ods_dev.ods_oitm d on b.itementry=d.docentry
where c.shpcode is not null and d.itemcode is not null and to_date(b.duedate)='{0}' and c.shpcode in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
union all
select 
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
current_timestamp() etl_time,
substring(a.exitdate,1,7) year_month
from rbu_sxcp_ods_dev.ods_wsl1 a
left join rbu_sxcp_ods_dev.ods_oshp b on a.shpentry=b.docentry
left join rbu_sxcp_ods_dev.ods_oitm c on a.itementry=c.docentry 
where b.Shpcode is not null and c.Itemcode is not null and to_date(a.exitdate)='{0}' and b.Shpcode in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
union all
select 
b.Shpcode,
c.Itemcode,
sum(a.Qty1),
sum(a.LineTotal),
c.msrunit,
null,
case when hour(a.exitdate)<3 then to_date(date_add(a.exitdate,-1)) else to_date(a.exitdate) end,
null,
'出库',
'出库',
current_timestamp() etl_time,
substring(a.exitdate,1,7) year_month
from rbu_sxcp_ods_dev.ods_sal1 a
left join rbu_sxcp_ods_dev.ods_oshp b on a.shpentry=b.docentry
left join rbu_sxcp_ods_dev.ods_oitm c on a.itementry=c.docentry 
where b.Shpcode is not null and c.Itemcode is not null 
    and (case when hour(a.exitdate)<3 then to_date(date_add(a.exitdate,-1)) else to_date(a.exitdate) end)='{0}' and b.Shpcode in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
group by 
b.Shpcode,c.Itemcode,c.msrunit,
case when hour(a.exitdate)<3 then to_date(date_add(a.exitdate,-1)) else to_date(a.exitdate) end,
current_timestamp(),substring(a.exitdate,1,7)
union all
select 
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
current_timestamp() etl_time,
substring(t1.exitdate,1,7) year_month
from rbu_sxcp_ods_dev.ods_str1 t1 
left join rbu_sxcp_ods_dev.ods_oshp t2 
    on t1.shpoutentry=t2.docentry
left join rbu_sxcp_ods_dev.ods_oitm t4 
    on t1.itementry=t4.docentry
where t2.shpcode is not null 
    and t4.itemcode is not null 
    and to_date(t1.exitdate)='{0}' 
    and t2.shpcode in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
union all
select 
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
current_timestamp() etl_time,
substring(t1.exitdate,1,7) year_month
from rbu_sxcp_ods_dev.ods_str1 t1 
left join rbu_sxcp_ods_dev.ods_oshp t2 on t1.shpinentry=t2.docentry
left join rbu_sxcp_ods_dev.ods_oitm t4 on t1.itementry=t4.docentry
where t2.shpcode is not null and t4.itemcode is not null and to_date(t1.exitdate)='{0}' and t2.shpcode in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
union all
select 
c.Shpcode store_code,
d.itemcode product_code,
b.Qty1 qty,
b.linetotal amt,
d.msrunit unit,
a.docnum doc_no,
to_date(b.enterdate) io_date,
b.enterdate io_time,
'报损' io_type,
'产品报废' origin_type,
current_timestamp etl_time,
substring(b.enterdate,1,7) year_month
from rbu_sxcp_ods_dev.ods_owsr a
left join rbu_sxcp_ods_dev.ods_wsr1 b on a.docentry=b.docentry and a.enterdate=b.enterdate and a.shpentry=b.shpentry
left join rbu_sxcp_ods_dev.ods_oshp c on b.shpentry=c.docentry
left join rbu_sxcp_ods_dev.ods_oitm d on b.itementry=d.docentry 
where a.reason='产品报废' and c.shpcode is not null and d.itemcode is not null and to_date(b.enterdate)='{0}' and c.Shpcode 
in ('st_code_0585', 'st_code_0292', 'st_code_0659')) 
""".format(item)

# insert2_sql = """
# insert into table rbu_sxcp_edw_dev.fct_io
# (select
# b.Shpcode,
# c.Itemcode,
# a.Qty1,
# a.LineTotal,
# c.msrunit,
# NULL,
# to_date(a.exitdate),
# a.exitdate,
# '入库',
# '入库',
# current_timestamp()
# from rbu_sxcp_ods_dev.ods_wsl1 a
# left join rbu_sxcp_ods_dev.ods_oshp b on a.shpentry=b.docentry
# left join rbu_sxcp_ods_dev.ods_oitm c on a.itementry=c.docentry
# where b.Shpcode is not null and c.Itemcode is not null and to_date(a.exitdate)='{0}')
# """.format(item)
#
# insert3_sql = """
# insert into table rbu_sxcp_edw_dev.fct_io
# (select
# b.Shpcode,
# c.Itemcode,
# a.Qty1,
# a.LineTotal,
# c.msrunit,
# a.docnum,
# to_date(a.exitdate),
# a.exitdate,
# '出库',
# '出库',
# current_timestamp()
# from rbu_sxcp_ods_dev.ods_sal1 a
# left join rbu_sxcp_ods_dev.ods_oshp b on a.shpentry=b.docentry
# left join rbu_sxcp_ods_dev.ods_oitm c on a.itementry=c.docentry
# where b.Shpcode is not null and c.Itemcode is not null and to_date(a.exitdate)='{0}')
# """.format(item)
#
# insert4_sql = """
# insert into table rbu_sxcp_edw_dev.fct_io
# (select
# t2.shpcode,
# t4.itemcode,
# t1.qty1,
# t1.LineTotal,
# t4.msrunit,
# NULL,
# to_date(t1.exitdate),
# t1.exitdate,
# '调拨出库',
# '调拨',
# current_timestamp()
# from rbu_sxcp_ods_dev.ods_str1 t1
# left join rbu_sxcp_ods_dev.ods_oshp t2 on t1.shpoutentry=t2.docentry
# left join rbu_sxcp_ods_dev.ods_oitm t4 on t1.itementry=t4.docentry
# where t2.shpcode is not null and t4.itemcode is not null and to_date(t1.exitdate)='{0}')
# """.format(item)
#
# insert5_sql = """
# insert into table rbu_sxcp_edw_dev.fct_io
# (select
# t2.shpcode,
# t4.itemcode,
# t1.qty1,
# t1.LineTotal,
# t4.msrunit,
# NULL,
# to_date(t1.exitdate),
# t1.exitdate,
# '调拨入库',
# '调拨',
# current_timestamp()
# from rbu_sxcp_ods_dev.ods_str1 t1
# left join rbu_sxcp_ods_dev.ods_oshp t2 on t1.shpinentry=t2.docentry
# left join rbu_sxcp_ods_dev.ods_oitm t4 on t1.itementry=t4.docentry
# where t2.shpcode is not null and t4.itemcode is not null and to_date(t1.exitdate)='{0}')
# """.format(item)
print("开始插入数据")
start = time.time()
spark.sql(insert1_sql)
print("插入订单信息成功")
# spark.sql(insert2_sql)
# print("插入入库信息成功")
# spark.sql(insert3_sql)
# print("插入出库信息成功")
# spark.sql(insert4_sql)
# print("插入调拨入库信息成功")
# spark.sql(insert5_sql)
# print("插入调拨出库信息成功")
end = time.time()
print('Running time: %s Seconds' % (end - start))

print("process successfully!")
print("=====================")