# dim_product
#  使用yarn调度 

```
# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext, HiveContext,UDFRegistration
from pyspark.sql.functions import udf
import datetime
import re
import edw_module
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
# import sys


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


sc = SparkContext(appName="PythonSQL", master="yarn")
sqlContext = SQLContext(sc)
hc = HiveContext(sc)

get_phone_number = hc.udf.register("get_phone_number", get_number)
get_shelf_life = hc.udf.register("get_shelf_life", get_shelf_life)
convert_material = hc.udf.register("convert_material", convert_material)


sql_ods_oitm = """
select  ItemCode,ItemName,a8,MsrUnit,warranty,Stuff,ItemLive,Cancel from rbu_sxcp_ods_dev.ods_oitm
"""
df_ods_oitm = hc.sql(sql_ods_oitm)
df_ods_oitm.registerTempTable("tmp_ods_oitm_spark")

print("starting..........")
sql_create_dim_product_1 = """
create table rbu_sxcp_edw_dev.dim_product_1
(product_code string,
product_name string,
large_class string,
middle_class string,
small_class string,
unit string,
warranty decimal (30, 8),
shelf_life decimal (30, 8),
is_material int,
is_valid int,
min_order_qty decimal (30, 8),
min_package_qty decimal (30, 8),
safety_stock_qty decimal (30, 8),
fixed_days_supply int,
etl_time string
)
stored as  parquet
location '/liufuya/edw.db/dim_product_1'
"""
#sqlContext.sql(sql_create_dim_product_1)
print("创建表成功")

insert_sql = """
insert overwrite table rbu_sxcp_edw_dev.dim_product_1
(select 
distinct a.itemcode product_code,
a.ItemName product_name,
a.a8 large_class,
NULL,
NULL,
a.MsrUnit unit,
get_phone_number(a.warranty) warranty,
get_shelf_life(get_phone_number(a.warranty)) shelf_life,
convert_material(a.Stuff) is_material,
1 is_valid,
a.ItemLive min_order_qty,
NULL,
NULL,
NULL,
current_timestamp()
from tmp_ods_oitm_spark a 
where a.cancel='0')
"""
result = hc.sql(insert_sql)
print("插入数据成功")
df_ods_oitm.drop()
print("删除临时表成功")
print("process successfully!")
print("=====================")
```