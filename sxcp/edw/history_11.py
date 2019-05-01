# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import datetime

spark = SparkSession.builder.master("yarn").appName("fct_store_product_on_sale").enableHiveSupport().getOrCreate()

print("starting..........")

sql_pos_detail = """
SELECT store_code,product_code, sale_date
FROM rbu_sxcp_edw_dev.fct_pos_detail
where store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
"""
df_pos_detail = spark.sql(sql_pos_detail)
df_pos_detail = df_pos_detail.dropDuplicates()
df_pos_detail.createOrReplaceTempView("tmp_pos_detail_spark")
spark.catalog.cacheTable("tmp_pos_detail_spark")
print("获取销售明细数据成功")

sql_dim_store = """
select * from rbu_sxcp_edw_dev.dim_store where store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
"""
df_dim_store = spark.sql(sql_dim_store)
df_dim_store.createOrReplaceTempView("tmp_dim_store_spark")
spark.catalog.cacheTable("tmp_dim_store_spark")
print("获取门店数据成功")

sql_dim_product = """
select * from rbu_sxcp_edw_dev.dim_product
"""
df_dim_product = spark.sql(sql_dim_product)
df_dim_product.createOrReplaceTempView("tmp_dim_product_spark")
spark.catalog.cacheTable("tmp_dim_product_spark")
print("获取商品数据成功")

sql_ods_product_bill_shop = """
select shop_code,product_code,bill_date from rbu_sxcp_ods_dev.ods_product_bill_shop
"""
df_ods_product_bill_shop = spark.sql(sql_ods_product_bill_shop)
df_ods_product_bill_shop.createOrReplaceTempView("tmp_ods_product_bill_shop")
spark.catalog.cacheTable("tmp_ods_product_bill_shop")
print("获取ods_product_bill_shop数据成功")


sql_tmp11 = """
select product_code,
case when warranty=1 then 1
     when warranty=2 then 1
     when warranty=3 then 1
     when warranty=4 then 1
     when warranty=5 then 2
     when warranty=6 then 3
     when warranty=7 then 4
     else warranty
end best_life
from tmp_dim_product_spark
"""
df_tmp11 = spark.sql(sql_tmp11)
df_tmp11.createOrReplaceTempView("tmp1")

sql_tmp22 = """
select product_code,
case when product_code='pr_code_1421' then 1
     when product_code='pr_code_0564' then 1
     when product_code='pr_code_1269' then 1
     else best_life
end best_life
from tmp1
"""
df_tmp22 = spark.sql(sql_tmp22)
df_tmp22.createOrReplaceTempView("tmp22")
spark.catalog.cacheTable("tmp22")
print("获取产品的best_life数据")


def jiaoben(item):
    tmp1_sql = """
    select 
    t1.store_code,
    t2.store_name,
    t1.product_code,
    t3.product_name,
    t1.sale_date
    from tmp_pos_detail_spark t1
    left join tmp_dim_store_spark t2 on t1.store_code=t2.store_code
    left join tmp_dim_product_spark t3 on t3.product_code=t1.product_code
    where sale_date>=date_sub('{0}',30) and sale_date<='{0}'
    """.format(item)
    df_tmp1 = spark.sql(tmp1_sql)
    df_tmp1.createOrReplaceTempView("tmp1_spark")

    tmp2_sql = """
    select
    store_code,
    product_code,
    '{0}' check_date,
    max(sale_date) before_date
    from tmp1_spark
    where sale_date<'{0}'
    group by store_code,product_code,'{0}'
    """.format(item)

    df_tmp2 = spark.sql(tmp2_sql)
    df_tmp2.createOrReplaceTempView("tmp2_spark")

    tmp3_sql = """
    select
    store_code,
    product_code,
    check_date,
    before_date
    from tmp2_spark a
    where date_sub(check_date,7)>before_date
    and not exists
    (
        select 1 from tmp_ods_product_bill_shop b
        where a.store_code=b.shop_code
            and a.product_code=b.product_code
            and b.bill_date>=date_sub('{0}',1)
            and b.bill_date<='{0}'
    )
    """.format(item)

    df_tmp3 = spark.sql(tmp3_sql)
    df_tmp3.createOrReplaceTempView("tmp3_spark")

    sql_final = """
    select
    a.store_code,
    a.store_name,
    a.product_code,
    a.product_name,
    b.large_class,
    b.unit,
    c.sale_price,
    c.purchase_price,
    b.shelf_life,
    b.min_order_qty,
    c.min_display_qty,
    current_timestamp() etl_time
    from tmp1_spark a
    left join tmp3_spark d on a.store_code=d.store_code and a.product_code=d.product_code
    left join tmp_dim_product_spark b on a.product_code=b.product_code
    left join rbu_sxcp_edw_dev.dim_store_product c on c.store_code=a.store_code and c.product_code=a.product_code
    where d.store_code is null and  a.store_code is not null and a.store_name is not null and a.product_code is not null and a.product_name is not null
    """
    df_final = spark.sql(sql_final)
    df_final = df_final.dropDuplicates()
    df_final.createOrReplaceTempView("final")
    insert_sql = """
    insert overwrite table rbu_sxcp_edw_dev.fct_store_product_on_sale
    (select
    store_code,
    store_name,
    product_code,
    product_name,
    large_class,
    unit,
    sale_price,
    purchase_price,
    shelf_life,
    min_order_qty,
    min_display_qty,
    etl_time
    from final)
    """
    print("on_sale开始插入每天数据")
    #spark.sql(insert_sql)
    print("on_sale开始插入每天数据成功")

    insert_history_sql = """
    insert into table rbu_sxcp_edw_dev.fct_store_product_on_sale_history
    (select 
    store_code,
    store_name,
    a.product_code,
    product_name,
    large_class,
    '{0}' sale_date,
    sale_price, 
    purchase_price,
    shelf_life,
    unit,
    min_order_qty, 
    min_display_qty, 
    b.best_life
    from final a
    left join tmp22 b on a.product_code=b.product_code)
    """.format(item)
    print("开始插入on_sale_history")
    spark.sql(insert_history_sql)
    print("插入on_sale_history成功")
    df_pos_detail.drop()
    df_tmp1.drop()
    df_tmp2.drop()
    df_tmp3.drop()
    df_ods_product_bill_shop.drop()
    df_dim_product.drop()
    df_dim_store.drop()
    print("删除临时表成功")


for i in range(0, 30):
    print("on_sale和on_sale_history")
    min_day = datetime.datetime.strptime('2018-11-01', "%Y-%m-%d") + datetime.timedelta(days=i)
    print(str(min_day)[:10])
    jiaoben(str(min_day)[:10])

spark.catalog.uncacheTable("tmp22")
spark.catalog.uncacheTable("tmp_ods_product_bill_shop")
spark.catalog.uncacheTable("tmp_dim_product_spark")
spark.catalog.uncacheTable("tmp_dim_store_spark")
spark.catalog.uncacheTable("tmp_pos_detail_spark")
print("删除缓存表成功")
print("process successfully!")
print("=====================")
