# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext, HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import datetime
import re


def get_number(item):
    res = re.search(r'\d+', str(item))
    if res:
        return str(res.group())
    return '0'


def get_shelf_life(item):
    if item:
        if 1 < int(item) < 8:
            return int(item)-1
        else:
            return int(item)
    return 1


def convert_material(item):
    if item == '产品':
        return 1
    return 0


spark = SparkSession.builder.master("yarn").appName("dim_product").enableHiveSupport().getOrCreate()


get_phone_number = spark.udf.register("get_phone_number", get_number)
get_shelf_life = spark.udf.register("get_shelf_life", get_shelf_life)
convert_material = spark.udf.register("convert_material", convert_material)


sql_ods_oitm = """
select  ItemCode,ItemName,a8,MsrUnit,warranty,Stuff,ItemLive,Cancel from rbu_sxcp_ods_dev.ods_oitm
"""
df_ods_oitm = spark.sql(sql_ods_oitm)
df_ods_oitm.createOrReplaceTempView("tmp_ods_oitm_spark")


final_sql = """
select 
distinct a.itemcode product_code,
a.ItemName product_name,
a.a8 large_class,
a.MsrUnit unit,
get_phone_number(a.warranty) warranty,
get_shelf_life(get_phone_number(a.warranty)) shelf_life,
convert_material(a.Stuff) is_material,
1 is_valid,
a.ItemLive min_order_qty
from tmp_ods_oitm_spark a 
where a.cancel='0' and a.itemcode is not null and a.ItemName is not null
"""
df_final = spark.sql(final_sql)
df_final = df_final.dropDuplicates()
df_final.createOrReplaceTempView("final")

print("starting..........")
insert_sql = """
insert overwrite table rbu_sxcp_edw_dev.dim_product
(select 
product_code,
product_name,
large_class,
NULL,
NULL,
unit,
warranty,
shelf_life,
is_material,
is_valid,
min_order_qty,
NULL,
NULL,
NULL,
current_timestamp()
from final)
"""
spark.sql(insert_sql)
print("插入数据成功")
df_ods_oitm.drop()
print("删除临时表成功")
print("process successfully!")
print("=====================")