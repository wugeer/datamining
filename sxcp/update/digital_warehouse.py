# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
from datetime import datetime, date, timedelta
import re
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
import time

spark = SparkSession.builder.master("yarn").appName("digital_warehouse").enableHiveSupport().getOrCreate()
item = (date.today() + timedelta(days=-2)).strftime("%Y-%m-%d")

print("process starting..........")
print("开始生成%s 库龄数据"%item)
sql_store_sale_add = """
select 
    store_code,
    product_code,
    check_date,
    end_qty,
    sale_qty,
    loss_qty
from rbu_sxcp_edw_ai_dev.store_sale_add
where check_date='{0}'
""".format(item)
df_store_sale_add = spark.sql(sql_store_sale_add)
df_store_sale_add.createOrReplaceTempView("fct_store_sale_add")
print("打印销补行数")
print(df_store_sale_add.count())
spark.catalog.cacheTable("fct_store_sale_add")
#spark.catalog.cacheTable("fct_store_sale_add")

# 得到货架期大于0的商品
sql_dim_product = """
select 
    product_code,
    shelf_life
from rbu_sxcp_edw_dev.dim_product
where shelf_life>0
"""
df_dim_product = spark.sql(sql_dim_product)
df_dim_product.createOrReplaceTempView("dim_product")
print("打印到货架期大于0的商品行数")
print(df_dim_product.count())
spark.catalog.cacheTable("dim_product")
#spark.catalog.cacheTable("dim_product")
# print("打印门店商品的货架期")
# df_dim_product.show()

sql_fct_io = """
select 
    store_code,
    product_code,
    io_type,
    io_date check_date,
    sum(qty) qty
from rbu_sxcp_edw_dev.fct_io
where io_date='{0}' and io_type='入库'
group by 
    store_code,
    product_code,
    io_type,
    io_date
""".format(item)
df_fct_io = spark.sql(sql_fct_io)
df_fct_io.createOrReplaceTempView("fct_io")
print("打印io表的行数")
print(df_fct_io.count())
spark.catalog.cacheTable("fct_io")
#spark.catalog.cacheTable("fct_io")
# print("打印入库数据")
# df_fct_io.show()


def split_part(item, sp, num):
    if item:
        res = item.split(sp)
        if len(res) >= num:
            return res[num-1]


split_part = spark.udf.register("split_part", split_part)

tmp1_sql = """
select t1.store_code,
t1.product_code,
t1.check_date,
t2.shelf_life,
coalesce(t1.end_qty,0) end_qty,
t3.qty in_qty,
t1.sale_qty,
t1.loss_qty
from fct_store_sale_add t1
left join dim_product t2 on t1.product_code=t2.product_code
left join fct_io t3 on t1.check_date=t3.check_date and t1.store_code=t3.store_code and t1.product_code=t3.product_code and t3.io_type='入库'
where t2.shelf_life<=7 and t1.check_date='{0}' and t2.shelf_life is not null
""".format(item)
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1")
print("打印tmp1行数")
print(df_tmp1.count())
spark.catalog.cacheTable("tmp1")

sql_final2 = """
select 
    t1.store_code,
    t1.product_code,
    t1.check_date,
    t2.shelf_life,
    t1.end_qty
from fct_store_sale_add t1
left join dim_product t2 
    on t1.product_code=t2.product_code
where t2.shelf_life>7 
    and t1.check_date='{0}'
""".format(item)
df_final2 = spark.sql(sql_final2)
#df_final2 = df_final2.dropDuplicates()
df_final2.createOrReplaceTempView("final2")
print("打印常保品的行数")
print(df_final2.count())

spark.catalog.cacheTable("final2")
spark.catalog.uncacheTable("fct_store_sale_add")
spark.catalog.uncacheTable("fct_io")
spark.catalog.uncacheTable("dim_product")

# print("打印tmp1")
# df_tmp1.show()

# 取出昨日库龄
tmp2a_sql = """
select store_code,
product_code,
check_date,
shelf_life,
end_qty,
digital_warehouse,
year_month
from rbu_sxcp_edw_ai_dev.digital_warehouse
where shelf_life<=7 and check_date=date_sub('{0}',1) and end_qty<>0 and store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
""".format(item)
df_tmp2a = spark.sql(tmp2a_sql)
df_tmp2a.createOrReplaceTempView("tmp2a")
print("打印tmp2a行数")
print(df_tmp2a.count())
spark.catalog.cacheTable("tmp2a")
#print("打印昨日库龄")
# df_tmp2a.show()

tmp2_sql = """
select coalesce(t3.store_code,t4.store_code) store_code,
coalesce(t3.product_code,t4.product_code) product_code,
coalesce(date_add(t3.check_date, 1),t4.check_date) check_date,
coalesce(t3.shelf_life,t4.shelf_life) shelf_life,
case when t3.shelf_life>1 then split_part(t3.digital_warehouse,',',2)
    when t3.shelf_life=1 then 0
    else null end day_1_qty,
case when t3.shelf_life>2 then split_part(t3.digital_warehouse,',',3)
    when t3.shelf_life=2 then 0
    else null end day_2_qty,
case when t3.shelf_life>3 then split_part(t3.digital_warehouse,',',4)
    when t3.shelf_life=3 then 0
    else null end day_3_qty,
case when t3.shelf_life>4 then split_part(t3.digital_warehouse,',',5)
    when t3.shelf_life=4 then 0
    else null end day_4_qty,
case when t3.shelf_life>5 then split_part(t3.digital_warehouse,',',6)
    when t3.shelf_life=5 then 0
    else null end day_5_qty,
case when t3.shelf_life>6 then split_part(t3.digital_warehouse,',',7)
    when t3.shelf_life=6 then 0
    else null end day_6_qty,
case when t3.shelf_life=7 then 0
    else null end day_7_qty,
coalesce(t4.sale_qty,0) sale_qty,
coalesce(t4.in_qty,0) in_qty,
coalesce(t4.end_qty,0) end_qty
from tmp2a t3
full join tmp1 t4 on t3.store_code=t4.store_code and t3.product_code=t4.product_code 
"""
df_tmp2 = spark.sql(tmp2_sql)
df_tmp2.createOrReplaceTempView("tmp2")
print("打印tmp2行数")
print(df_tmp2.count())
spark.catalog.uncacheTable("tmp2a")
spark.catalog.uncacheTable("tmp1")
spark.catalog.cacheTable("tmp2")

# print("tmp2数据")
# df_tmp2.show()
print("出入库、日末库存为空即0")
tmp3_sql = """
select store_code,
product_code,
check_date,
shelf_life,
day_1_qty,
day_2_qty,
day_3_qty,
day_4_qty,
day_5_qty,
day_6_qty,
day_7_qty,
case when coalesce(day_1_qty,0)+coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)+coalesce(in_qty,0)-coalesce(sale_qty,0)>coalesce(end_qty,0) 
    then coalesce(day_1_qty,0)+coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)+coalesce(in_qty,0)-coalesce(end_qty,0)
    else coalesce(sale_qty,0) end sale_qty,
case when coalesce(day_1_qty,0)+coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)+coalesce(in_qty,0)-coalesce(sale_qty,0)<coalesce(end_qty,0) 
    then coalesce(end_qty,0)-(coalesce(day_1_qty,0)+coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)+coalesce(in_qty,0)-coalesce(sale_qty,0))
    else 0 end extra_qty,
coalesce(in_qty,0) in_qty,
coalesce(end_qty,0) end_qty
from tmp2
"""
df_tmp3 =spark.sql(tmp3_sql)
df_tmp3.createOrReplaceTempView("tmp3")
print("打印tmp3行数")
print(df_tmp3.count())
spark.catalog.uncacheTable("tmp2")
spark.catalog.cacheTable("tmp3")

# print("tmp3")
# df_tmp3.show()

print("计算入库")
tmp4_sql = """
select store_code,
product_code,
check_date,
shelf_life,
case when shelf_life>1 then coalesce(day_1_qty,0)
    else in_qty+extra_qty end day_1_qty,
case when shelf_life>2 then coalesce(day_2_qty,0)+extra_qty
    when shelf_life=2 then in_qty+extra_qty
    else null end day_2_qty,
case when shelf_life>3 then coalesce(day_3_qty,0)
    when shelf_life=3 then in_qty
    else null end day_3_qty,
case when shelf_life>4 then coalesce(day_4_qty,0)
    when shelf_life=4 then in_qty
    else null end day_4_qty,
case when shelf_life>5 then coalesce(day_5_qty,0)
    when shelf_life=5 then in_qty
    else null end day_5_qty,
case when shelf_life>6 then coalesce(day_6_qty,0)
    when shelf_life=6 then in_qty
    else null end day_6_qty,		
case when shelf_life=7 then in_qty
    else null end day_7_qty,
sale_qty,
end_qty
from tmp3
"""
df_tmp4 = spark.sql(tmp4_sql)
df_tmp4.createOrReplaceTempView("tmp4")
print("打印tmp4行数")
print(df_tmp4.count())
spark.catalog.uncacheTable("tmp3")
spark.catalog.cacheTable("tmp4")
# print("tmp4")
# df_tmp4.show()

print("计算出库(累和)")
tmp5_sql = """
select store_code,
product_code,
check_date,
shelf_life,
end_qty,
case when day_1_qty-sale_qty<=0 then 0 
    when day_1_qty-sale_qty>0 then day_1_qty-sale_qty 
end day_1_sum,
case when day_1_qty+day_2_qty-sale_qty<=0 then 0
    when day_1_qty+day_2_qty-sale_qty>0 then day_1_qty+day_2_qty-sale_qty 
end day_2_sum,
case when day_1_qty+day_2_qty+day_3_qty-sale_qty<=0 then 0
    when day_1_qty+day_2_qty+day_3_qty-sale_qty>0 then day_1_qty+day_2_qty+day_3_qty-sale_qty 
end day_3_sum,
case when day_1_qty+day_2_qty+day_3_qty+day_4_qty-sale_qty<=0 then 0
    when day_1_qty+day_2_qty+day_3_qty+day_4_qty-sale_qty>0 then day_1_qty+day_2_qty+day_3_qty+day_4_qty-sale_qty 
end day_4_sum,
case when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty-sale_qty<=0 then 0
    when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty-sale_qty>0 then day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty-sale_qty 
end day_5_sum,
case when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty-sale_qty<=0 then 0
    when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty-sale_qty>0 then day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty-sale_qty 
end day_6_sum,
case when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty+day_7_qty-sale_qty<=0 then 0
    when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty+day_7_qty-sale_qty>0 then day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty+day_7_qty-sale_qty 
end day_7_sum
from tmp4
"""
df_tmp5 = spark.sql(tmp5_sql)
df_tmp5.createOrReplaceTempView("tmp5")
print("打印tmp5行数")
print(df_tmp5.count())
spark.catalog.uncacheTable("tmp4")
spark.catalog.cacheTable("tmp5")
# print("tmp5")
# df_tmp5.show()
print("展开每日库存")

tmp6_sql = """
select store_code,
product_code,
check_date,
shelf_life,
end_qty,
0+day_1_sum day_1_qty,
0+day_2_sum-day_1_sum day_2_qty,
0+day_3_sum-day_2_sum day_3_qty,
0+day_4_sum-day_3_sum day_4_qty,
0+day_5_sum-day_4_sum day_5_qty,
0+day_6_sum-day_5_sum day_6_qty,
0+day_7_sum-day_6_sum day_7_qty
from tmp5
"""
df_tmp6 = spark.sql(tmp6_sql)
df_tmp6.createOrReplaceTempView("tmp6")
print("打印tmp6行数")
spark.catalog.cacheTable("tmp6")
print(df_tmp6.count())
spark.catalog.uncacheTable("tmp5")

# print("tmp6")
# df_tmp6.show()


#dim_store_product_sql = """
#select store_code,product_code,shelf_life
#from r#bu_sxcp_edw_dev.dim_store_product
#where store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659')
#"""
#df_store_product = spark.sql(dim_store_product_sql)
#df_store_product.createOrReplaceTempView("store_product")
##print("打印store_product行数")
##print(df_store_product.count())
#spark.catalog.cacheTable("store_product")

print("6789")
#print(df_tmp6.count())
#df_tmp6.cache
#print(df_store_product.count())
#df_store_product.cache
print("1234")

#def my_coalesce(item):
#    if item:
#        return item
#
#
#spark.udf.register("my_coalesce", my_coalesce)


print("判断货架期是否变动，如变动则调整库龄，增加则补0，减少则压缩减少的天数")
#tmp7_sql = """
#select
#t7.store_code,
#t7.product_code,
#t7.check_date,
#t8.shelf_life,
#t7.end_qty,
#case
#    when t7.shelf_life>1 and t8.shelf_life=1 then coalesce(day_1_qty,0)+coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
#    else day_1_qty
#end day_1_qty,
#case
#    when t7.shelf_life<2 and t8.shelf_life>=2 then 0
#    when t7.shelf_life>=2 and t8.shelf_life<2 then null
#    when t7.shelf_life>2 and t8.shelf_life=2 then coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
#    else day_2_qty
#end day_2_qty,
#case
#    when t7.shelf_life<3 and t8.shelf_life>=3 then 0
#    when t7.shelf_life>=3 and t8.shelf_life<3 then null
#    when t7.shelf_life>3 and t8.shelf_life=3 then coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
#    else day_3_qty
#end day_3_qty,
#case
#    when t7.shelf_life<4 and t8.shelf_life>=4 then 0
#    when t7.shelf_life>=4 and t8.shelf_life<4 then null
#    when t7.shelf_life>4 and t8.shelf_life=4 then coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
#    else day_4_qty
#end day_4_qty,
#case
#    when t7.shelf_life<5 and t8.shelf_life>=5 then 0
#    when t7.shelf_life>=5 and t8.shelf_life<5 then null
#    when t7.shelf_life>5 and t8.shelf_life=5 then coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
#    else day_5_qty
#end day_5_qty,
#case
#    when t7.shelf_life<6 and t8.shelf_life>=6 then 0
#    when t7.shelf_life>=6 and t8.shelf_life<6 then null
#    when t7.shelf_life>6 and t8.shelf_life=6 then coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
#    else day_6_qty
#end day_6_qty,
#case
#    when t7.shelf_life<7 and t8.shelf_life>=7 then 0
#    when t7.shelf_life>=7 and t8.shelf_life<7 then null
#    else day_7_qty
#end day_7_qty
#from tmp6 t7
#left join rbu_sxcp_edw_dev.dim_store_product t8
#    on t7.store_code=t8.store_code
#    and t7.product_code=t8.product_code
#"""
#df_tmp7 = spark.sql(tmp7_sql)
#df_tmp7.createOrReplaceTempView("tmp7")
#spark.catalog.cacheTable("tmp7")
#print("打印tmp7行数")
#print(df_tmp7.count())
#spark.catalog.uncacheTable("store_product")
#spark.catalog.uncacheTable("tmp6")

# print("tmp7")
# df_tmp7.show()

final_sql = """
select 
    store_code,
    product_code,
    check_date,
    shelf_life,
    end_qty,
case 
    when shelf_life=1 then day_1_qty
    when shelf_life=2 then concat_ws(',',cast(day_1_qty as string), cast(day_2_qty as string))
    when shelf_life=3 then concat_ws(',',cast(day_1_qty as string), cast(day_2_qty as string), cast(day_3_qty as string))  
    when shelf_life=4 then concat_ws(',',cast(day_1_qty as string), cast(day_2_qty as string), cast(day_3_qty as string), cast(day_4_qty as string))  
    when shelf_life=5 then concat_ws(',',cast(day_1_qty as string), cast(day_2_qty as string), cast(day_3_qty as string), cast(day_4_qty as string), cast(day_5_qty as string)) 
    when shelf_life=6 then concat_ws(',',cast(day_1_qty as string), cast(day_2_qty as string), cast(day_3_qty as string), cast(day_4_qty as string), cast(day_5_qty as string), cast(day_6_qty as string))
    when shelf_life=7 then concat_ws(',',cast(day_1_qty as string), cast(day_2_qty as string), cast(day_3_qty as string), cast(day_4_qty as string), cast(day_5_qty as string), cast(day_6_qty as string), cast(day_7_qty as string))
end digital_warehouse
from tmp6
where check_date='{0}'
""".format(item)
print("最终插入数据")
df_final = spark.sql(final_sql)
df_final = df_final.dropDuplicates()
df_final.createOrReplaceTempView("final")
print("打印货架期小于7的数据最终行数")
#spark.catalog.uncacheTable("tmp7")
#spark.catalog.cacheTable("final")
print(df_final.count())

#spark.sql("select * from final where check_date is null").show()
#spark.sql("select * from final where check_date is not null").show()

sql_zuizhong = """
SELECT 
    store_code, 
    product_code, 
    check_date, 
    shelf_life, 
    end_qty, 
    digital_warehouse, 
    etl_time, 
    year_month
FROM rbu_sxcp_edw_ai_dev.digital_warehouse
where check_date<'{0}' 
    and check_date>=concat(substring('{0}',1,7),'-01')
union all
select
    store_code,
    product_code,
    check_date,
    shelf_life,
    end_qty,
    digital_warehouse,
    current_timestamp() etl_time,
    substring(check_date,1,7) year_month
from final
union all
select
    store_code,
    product_code,
    check_date,
    shelf_life,
    end_qty,
    NULL digital_warehouse,
    current_timestamp() etl_time,
    substring(check_date,1,7) year_month
from final2
""".format(item)
df_zuizhong = spark.sql(sql_zuizhong)
df_zuizhong.createOrReplaceTempView("zuizhong")
spark.catalog.cacheTable("zuizhong")
print("打印最终插入该分区数据的行数")
print(df_zuizhong.count())
spark.catalog.uncacheTable("final2")
spark.catalog.uncacheTable("final")

spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
#spark.sql("set hive.exec.reducers.bytes.per.reducer=1024000000")
insert1_sql = """
insert overwrite table rbu_sxcp_edw_ai_dev.digital_warehouse partition(year_month)
(
select 
    store_code,
    product_code,
    check_date,
    shelf_life,
    end_qty,
    digital_warehouse,
    etl_time,
    year_month
from zuizhong
)
""".format(item)
start = time.time()
print("开始插入数据")
#spark.sql(insert1_sql)
end = time.time()
print("持续了%s" % (start-end))
spark.catalog.uncacheTable("zuizhong")
print("插入货架期小于七天的数据和常保品成功")
df_tmp1.drop()
df_tmp2.drop()
df_tmp2a.drop()
df_tmp3.drop()
df_tmp4.drop()
df_tmp5.drop()
df_tmp6.drop()
#df_tmp7.drop()
print("删除临时表成功")

print("process successfully!")
print("=====================")