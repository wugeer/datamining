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
pos_detail_sql = """
SELECT store_code, product_code, qty, discounted_price, amt, sale_date
FROM rbu_sxcp_edw_dev.fct_pos_detail
where sale_date>='2018-08-20'
"""
df_pos_detail = spark.sql(pos_detail_sql)
df_pos_detail.createOrReplaceTempView("pos_detail")
spark.catalog.cacheTable("pos_detail")

print("测试一下七天销量排名")
def jiaoben(item):
    # 七天销量
    tmp1_sql = """
    select store_code, product_code, amt
    from pos_detail
    where sale_date>date_sub('{0}', 7) and sale_date<='{0}'
    """.format(item)
    df_tmp1 = spark.sql(tmp1_sql)
    df_tmp1.createOrReplaceTempView("tmp1")

    tmp2_sql = """
    select store_code, product_code, sum(amt) total
    from tmp1
    group by store_code, product_code
    """
    df_tmp2 = spark.sql(tmp2_sql)
    df_tmp2.createOrReplaceTempView("tmp2")

    tmp3_sql = """
    select
    row_number() over(partition by store_code order by total desc) amt_id,
    store_code,
    product_code
    from tmp2
    """
    df_tmp3 = spark.sql(tmp3_sql)
    df_tmp3.createOrReplaceTempView("tmp3")

    tmp4_sql = """
    select store_code, product_code, amt,qty, discounted_price,sale_date
    from pos_detail
    where sale_date='{0}'
    """.format(item)
    df_tmp4 = spark.sql(tmp4_sql)
    df_tmp4.createOrReplaceTempView("tmp4")

    tmp5_sql = """
    select  store_code,
    product_code, 
    sum(amt) turnover,
    sum(qty) sale_total,
    discounted_price,
    sale_date
    from tmp4
    group by store_code,product_code,discounted_price,sale_date
    """
    df_tmp5 = spark.sql(tmp5_sql)
    df_tmp5.createOrReplaceTempView("tmp5")

    res_sql = """
    select
    a.store_code,
    a.product_code,
    a.sale_date,
    a.sale_total,
    a.turnover,
    a.discounted_price,
    amt_id
    from tmp5 a
    left join tmp3 b on a.store_code=b.store_code and a.product_code=b.product_code
    """
    df_res = spark.sql(res_sql)
    df_res.createOrReplaceTempView("res")

    insert_sql = """
    insert into table rbu_sxcp_rst_dev.seven_days_turnover_ranking
    (select 
    store_code,
    product_code,
    sale_date,
    sale_total,
    discounted_price,
    turnover,
    amt_id,
    current_timestamp()
    from res)
    """
    print("开始插入数据")
    spark.sql(insert_sql)
    print("插入数据成功")


for i in range(0, 3):
    print("seven_days_turnover_ranking")
    min_day = datetime.datetime.strptime('2018-09-01', "%Y-%m-%d") + datetime.timedelta(days=i)
    print(str(min_day)[:10])
    jiaoben(str(min_day)[:10])


spark.catalog.uncacheTable("pos_detail")
print("释放缓存表成功")
print("process successfully!")
print("=====================")

