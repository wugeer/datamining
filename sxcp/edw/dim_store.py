from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession

spark = SparkSession.builder.master("yarn").appName("dim_product").enableHiveSupport().getOrCreate()
print("starting..........")
sql_ods_ohem = """
select empcode, empname from rbu_sxcp_ods_dev.ods_ohem
"""
sql_ods_oshp = """
select 
shpcode,
shpname,
shptype,
shpclass,
listnum,
address,
area,
province,
city,
cityarea,
transline,
cycle,
startdate,
tel1,
starttime,
empdutyid,
endtime,
isinvalid
from rbu_sxcp_ods_dev.ods_oshp
"""
sql_ods_ousr = """
select usercode,username from rbu_sxcp_ods_dev.ods_ousr
"""
df_ods_ohem = spark.sql(sql_ods_ohem)
df_ods_ousr = spark.sql(sql_ods_ousr)
df_ods_oshp = spark.sql(sql_ods_oshp)

df_ods_oshp.createOrReplaceTempView("tmp_ods_oshp_spark")
df_ods_ohem.createOrReplaceTempView("tmp_ods_ohem_spark")
df_ods_ousr.createOrReplaceTempView("tmp_ods_ousr_spark")


final_sql = """
select 
t1.shpcode,
t1.shpname,
t1.shptype,
t1.shpclass,
t1.listnum,
t1.address,
t1.area,
t1.province,
t1.city,
t1.cityarea,
t1.transline,
t2.empcode,
t2.empname,
t1.cycle,
t1.startdate,
t1.tel1,  
t1.starttime,
t1.endtime,
t5.usercode
from tmp_ods_oshp_spark t1
left join tmp_ods_ohem_spark t2 on t1.empdutyid=t2.empcode
left join tmp_ods_ousr_spark t5 on t2.empname=t5.username
where t1.isinvalid='0' and t1.shpcode is not null and t1.shpname is not null
"""
df_final = spark.sql(final_sql)
df_final = df_final.dropDuplicates()
df_final.createOrReplaceTempView("final")

sql_select = '''
insert overwrite table rbu_sxcp_edw_dev.dim_store
(select 
shpcode,
shpname,
shptype,
shpclass,
listnum,
address,
area,
province,
city,
cityarea,
NULL,
NULL,
transline,
empcode,
empname,
'1',
cycle,
startdate,
tel1,  
starttime,
endtime,
'1',
usercode,
current_timestamp()
from final)
'''
print("开始插入数据")
result = spark.sql(sql_select)
print("插入数据成功")
df_ods_oshp.drop()
df_ods_ohem.drop()
df_ods_ousr.drop()
print("删除临时表成功")

print("process successfully!")
print("=====================")