from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext, HiveContext,catalog,SparkSession

spark = SparkSession.builder.master("yarn").enableHiveSupport().appName("dim_store_product").getOrCreate()

sql_ods_oshp = """
select shpcode shop_code, listnum from rbu_sxcp_ods_dev.ods_oshp
"""

sql_ods_oitm = """
select * from rbu_sxcp_ods_dev.ods_oitm
"""

sql_ods_itm1 = """
select itementry,pricenum,price from rbu_sxcp_ods_dev.ods_itm1
"""

df_ods_oshp = spark.sql(sql_ods_oshp)
df_ods_oitm = spark.sql(sql_ods_oitm)
df_ods_itm1 = spark.sql(sql_ods_itm1)
df_ods_oshp.createOrReplaceTempView("tmp_ods_oshp_spark")
df_ods_oitm.createOrReplaceTempView("tmp_ods_oitm_spark")
df_ods_itm1.createOrReplaceTempView("tmp_ods_itm1_spark")


print("starting..........")

sql_tmp1_a = """
select 
distinct shop_code,
listnum
from tmp_ods_oshp_spark 
"""

df_tmp1_a = spark.sql(sql_tmp1_a)
df_tmp1_a.createOrReplaceTempView("tmp1_a_spark")

sql_tmp1_b = """
select distinct itemcode product_code from tmp_ods_oitm_spark
"""
df_tmp1_b = spark.sql(sql_tmp1_b)
df_tmp1_b.createOrReplaceTempView("tmp1_b_spark")

print("所有门店数据就绪")
hc_sql_product = """
select distinct product_code,product_name,is_valid,shelf_life,unit,min_order_qty from rbu_sxcp_edw_dev.dim_product
"""
df_edw_product = spark.sql(hc_sql_product)
df_edw_product.createOrReplaceTempView("edw_product_spark")

sql_tmp1 = """
select a.shop_code,a.listnum,b.product_code 
from tmp1_a_spark a 
cross join tmp1_b_spark b
"""

df_tmp1 = spark.sql(sql_tmp1)
df_tmp1.createOrReplaceTempView("tmp1_spark")


sql_tmp2 = """
select 
distinct c.shop_code,
c.listnum,
d.price price_sale,
e.itemcode product_code
from tmp_ods_oshp_spark c
left join tmp_ods_itm1_spark d on c.listnum=d.pricenum 
left join tmp_ods_oitm_spark e on d.itementry=e.docentry
where d.price>0 and e.cancel='false'
"""

df_tmp2 = spark.sql(sql_tmp2)
df_tmp2.createOrReplaceTempView("tmp2_spark")


sql_ods_customer_501 = """
select * from rbu_sxcp_ods_dev.ods_customer_501
"""
sql_ods_customer_801 = """
select * from rbu_sxcp_ods_dev.ods_customer_801
"""
sql_ods_sa_personuprice_501 = """
select * from rbu_sxcp_ods_dev.ods_sa_personuprice_501
"""
sql_ods_sa_personuprice_801 = """
select * from rbu_sxcp_ods_dev.ods_sa_personuprice_801
"""
df_ods_customer_501 = spark.sql(sql_ods_customer_501)
df_ods_customer_801 = spark.sql(sql_ods_customer_801)
df_ods_sa_personuprice_501 = spark.sql(sql_ods_sa_personuprice_501)
df_ods_sa_personuprice_801 = spark.sql(sql_ods_sa_personuprice_801)

df_ods_customer_501.createOrReplaceTempView("tmp_ods_customer_501_spark")
df_ods_customer_801.createOrReplaceTempView("tmp_ods_customer_801_spark")
df_ods_sa_personuprice_501.createOrReplaceTempView("tmp_ods_sa_personuprice_501_spark")
df_ods_sa_personuprice_801.createOrReplaceTempView("tmp_ods_sa_personuprice_801_spark")
print("注册临时表成功")

sql_tmp3_a = """
select 
cpersoncode,
cinvcode,
iinvnowcost,
dstartdate,
denddate,
row_number() over(partition by cpersoncode,cinvcode order by dstartdate desc) date_rank
from tmp_ods_sa_personuprice_801_spark 
"""
df_tmp3_a = spark.sql(sql_tmp3_a)
df_tmp3_a.createOrReplaceTempView("tmp3_a_spark")

select_sql_3 = """
select 
f.cCusCode shop_code,
g.cInvCode product_code,
g.iInvNowCost price_wholesale
from tmp_ods_customer_801_spark f
left join tmp3_a_spark g on f.cCusPPerson=g.cpersoncode and g.date_rank=1
"""

df_tmp3 = spark.sql(select_sql_3)
df_tmp3.createOrReplaceTempView("tmp3_spark")


sql_tmp4_a = """
select 
cpersoncode,
cinvcode,
iinvnowcost,
dstartdate,
denddate,
row_number() over(partition by cpersoncode,cinvcode order by dstartdate desc) date_rank
from tmp_ods_sa_personuprice_501_spark
"""
df_tmp4_a = spark.sql(sql_tmp4_a)
df_tmp4_a.createOrReplaceTempView("tmp4_a_spark")
select_sql_4 = """
select 
h.cCusCode shop_code,
i.cInvCode product_code,
i.iInvNowCost price_wholesale
from tmp_ods_customer_501_spark h
left join tmp4_a_spark i on h.cCusPPerson=i.cpersoncode and i.date_rank=1
"""
df_tmp4 = spark.sql(select_sql_4)
df_tmp4.createOrReplaceTempView("tmp4_spark")

sql_tmp5_a = """
select 
itemcode product_code,
price,
dprice,
row_number() over(partition by itemcode order by years desc) code_id
from tmp_ods_oitm_spark 
where cancel='0'
"""
df_tmp5_a = spark.sql(sql_tmp5_a)
df_tmp5_a.createOrReplaceTempView("tmp5_a_spark")

select_sql_5 = """
select 
j.shop_code,
j.product_code,
coalesce(k.price_sale,l.price) price_sale,
coalesce(m.price_wholesale,n.price_wholesale,l.dprice) price_wholesale
from tmp1_spark j
left join tmp2_spark k on j.shop_code=k.shop_code and j.product_code=k.product_code
left join tmp5_a_spark l on j.product_code=l.product_code and l.code_id=1
left join tmp3_spark m on j.shop_code=m.shop_code and j.product_code=m.product_code
left join tmp4_spark n on j.shop_code=n.shop_code and j.product_code=n.product_code
"""

df_tmp5 = spark.sql(select_sql_5)
df_tmp5.createOrReplaceTempView("tmp5_spark")

hc_sql_store = """
select * from rbu_sxcp_edw_dev.dim_store
"""
df_edw_store = spark.sql(hc_sql_store)
df_edw_store.createOrReplaceTempView("edw_store_spark")

final_sql = """
select 
t0.shop_code,
t1.store_name,
t1.store_class,
t0.product_code,
t2.product_name,
t0.price_sale,
t0.price_wholesale,
t2.shelf_life,
t2.unit,
t2.min_order_qty
from tmp5_spark t0
left join edw_store_spark t1 on t1.store_code=t0.shop_code
left join edw_product_spark t2 on t2.product_code=t0.product_code
where t1.store_name is not null and t0.shop_code is not null
"""
df_final = spark.sql(final_sql)
df_final = df_final.dropDuplicates()
df_final.createOrReplaceTempView("final")

insert_sql = """
insert overwrite table rbu_sxcp_edw_dev.dim_store_product
(
select 
shop_code,
store_name,
product_code,
product_name,
price_sale,
price_wholesale,
shelf_life,
unit,
min_order_qty,
NULL,
NULL,
null,
null,
null,
current_timestamp()
from final) 
"""
result = spark.sql(insert_sql)
print("插入数据成功")
df_ods_itm1.drop()
df_ods_oshp.drop()
df_edw_product.drop()
df_edw_store.drop()
df_ods_customer_501.drop()
df_ods_customer_801.drop()
df_ods_oitm.drop()
df_ods_sa_personuprice_501.drop()
df_ods_sa_personuprice_801.drop()
df_tmp1.drop()
df_tmp1_a.drop()
df_tmp2.drop()
df_tmp3.drop()
df_tmp4.drop()
df_tmp5.drop()
print("删除临时表成功")
print("process successfully!")
print("=====================")