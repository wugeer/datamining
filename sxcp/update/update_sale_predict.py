# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import time
from datetime import datetime, date, timedelta
import re
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
# import sys

spark = SparkSession.builder.master("yarn").appName("sale_predict").enableHiveSupport().getOrCreate()
item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
pre_item = (date.today() + timedelta(days=-7)).strftime("%Y-%m-%d")
print("开始更新%s sale_predict数据"%item)
tmp1_sql = """
select store_code,product_code,check_date,sale_total_qty qty
from rbu_sxcp_edw_ai_dev.store_sale_add
where check_date='{0}'
""".format(item)
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1 = df_tmp1.dropDuplicates()
df_tmp1.createOrReplaceTempView("tmp1")
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
# print("store_sale_add")
# df_tmp1.show()#
insert1_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_predict partition(sale_date)
(select 
a.store_code,
a.product_code, 
b.qty, 
a.d1,
a.d2, 
a.d3, 
a.d4, 
a.d5, 
a.d6, 
a.d7, 
coalesce(b.qty-a.d1,a.error1) error1, 
a.error2, 
a.error3, 
a.error4, 
a.error5, 
a.error6, 
a.error7,
a.sale_date
from rbu_sxcp_edw_ai_dev.sale_predict a 
left join tmp1 b on a.store_code=b.store_code and a.product_code=b.product_code and a.sale_date=b.check_date
where a.sale_date='{0}' and b.qty is not null )
""".format(item)

insert2_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_predict partition(sale_date)
(select 
a.store_code,
a.product_code, 
a.qty, 
a.d1,
a.d2, 
a.d3, 
a.d4, 
a.d5, 
a.d6, 
a.d7, 
a.error1, 
coalesce(b.qty-a.d2,a.error2) error2,
a.error3, 
a.error4, 
a.error5, 
a.error6, 
a.error7,
a.sale_date
from rbu_sxcp_edw_ai_dev.sale_predict a 
left join tmp1 b on a.store_code=b.store_code and a.product_code=b.product_code and a.sale_date=date_sub(b.check_date,1)
where a.sale_date=date_sub('{0}',1)  and b.qty is not null )
""".format(item)

insert3_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_predict partition(sale_date)
(select 
a.store_code,
a.product_code, 
a.qty, 
a.d1,
a.d2, 
a.d3, 
a.d4, 
a.d5, 
a.d6, 
a.d7, 
a.error1, 
a.error2,
coalesce(b.qty-a.d3,a.error3) error3, 
a.error4, 
a.error5, 
a.error6, 
a.error7,
a.sale_date
from rbu_sxcp_edw_ai_dev.sale_predict a 
left join tmp1 b on a.store_code=b.store_code and a.product_code=b.product_code and a.sale_date=date_sub(b.check_date,2)
where a.sale_date=date_sub('{0}',2)  and b.qty is not null )
""".format(item)

insert4_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_predict partition(sale_date)
(select 
a.store_code,
a.product_code, 
a.qty, 
a.d1,
a.d2, 
a.d3, 
a.d4, 
a.d5, 
a.d6, 
a.d7, 
a.error1, 
a.error2,
a.error3, 
coalesce(b.qty-a.d4,a.error4) error4, 
a.error5, 
a.error6, 
a.error7,
a.sale_date
from rbu_sxcp_edw_ai_dev.sale_predict a 
left join tmp1 b on a.store_code=b.store_code and a.product_code=b.product_code and a.sale_date=date_sub(b.check_date,3)
where a.sale_date=date_sub('{0}',3) and b.qty is not null)
""".format(item)

insert5_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_predict partition(sale_date)
(select 
a.store_code,
a.product_code, 
a.qty, 
a.d1,
a.d2, 
a.d3, 
a.d4, 
a.d5, 
a.d6, 
a.d7, 
a.error1, 
a.error2,
a.error3, 
a.error4, 
coalesce(b.qty-a.d5,a.error5) error5, 
a.error6, 
a.error7,
a.sale_date
from rbu_sxcp_edw_ai_dev.sale_predict a 
left join tmp1 b on a.store_code=b.store_code and a.product_code=b.product_code and a.sale_date=date_sub(b.check_date,4)
where a.sale_date=date_sub('{0}',4) and b.qty is not null)
""".format(item)

insert6_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_predict partition(sale_date)
(select 
a.store_code,
a.product_code, 
a.qty, 
a.d1,
a.d2, 
a.d3, 
a.d4, 
a.d5, 
a.d6, 
a.d7, 
a.error1, 
a.error2,
a.error3, 
a.error4, 
a.error5, 
coalesce(b.qty-a.d6,a.error6) error6, 
a.error7,
a.sale_date
from rbu_sxcp_edw_ai_dev.sale_predict a 
left join tmp1 b on a.store_code=b.store_code and a.product_code=b.product_code and a.sale_date=date_sub(b.check_date,5)
where a.sale_date=date_sub('{0}',5) and b.qty is not null)
""".format(item)

insert7_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.sale_predict partition(sale_date)
(select 
a.store_code,
a.product_code, 
a.qty, 
a.d1,
a.d2, 
a.d3, 
a.d4, 
a.d5, 
a.d6, 
a.d7, 
a.error1, 
a.error2,
a.error3, 
a.error4, 
a.error5, 
a.error6, 
coalesce(b.qty-a.d7,a.error7) error7,
a.sale_date
from rbu_sxcp_edw_ai_dev.sale_predict a 
left join tmp1 b on a.store_code=b.store_code and a.product_code=b.product_code and a.sale_date=date_sub(b.check_date,6)
where a.sale_date=date_sub('{0}',6) and b.qty is not null)
""".format(item)
print("开始更新d1销量预测")
spark.sql(insert1_sql)
print("开始更新d2销量预测")
spark.sql(insert2_sql)
print("开始更新d3销量预测")
spark.sql(insert3_sql)
print("开始更新d4销量预测")
spark.sql(insert4_sql)
print("开始更新d5销量预测")
spark.sql(insert5_sql)
print("开始更新d6销量预测")
spark.sql(insert6_sql)
print("开始更新d7销量预测")
spark.sql(insert7_sql)
print("插入数据成功")
print("process successfully!")
print("=====================")

