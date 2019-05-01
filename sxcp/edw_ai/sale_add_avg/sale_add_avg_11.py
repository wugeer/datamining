# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
import datetime
import re

import sys
from pyspark.sql import Window
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

spark = SparkSession.builder.master("yarn").appName("mid_sale_add_avg").enableHiveSupport().getOrCreate()


print("starting..........")
print("平均销补数据的代码")
sql_mid_sale_add_sum = """
select * from rbu_sxcp_edw_ai_dev.sale_add_sum
"""
df_mid_sale_add_sum = spark.sql(sql_mid_sale_add_sum)
df_mid_sale_add_sum.createOrReplaceTempView("mid_sale_add_sum_spark")
spark.catalog.cacheTable("mid_sale_add_sum_spark")


def jiaoben(item):
    tmp1_1_sql = """
    select row_number() over(partition by store_code,product_code,date_case order by sale_date desc) date_id,
    store_code,
    product_code,
    date_case,
    sale_date,
    max_hour,
    sum_3_sale,
    sum_4_sale,
    sum_5_sale,
    sum_6_sale,
    sum_7_sale,
    sum_8_sale,
    sum_9_sale,
    sum_10_sale,
    sum_11_sale,
    sum_12_sale,
    sum_13_sale,
    sum_14_sale,
    sum_15_sale,
    sum_16_sale,
    sum_17_sale,
    sum_18_sale,
    sum_19_sale,
    sum_20_sale,
    sum_21_sale,
    sum_22_sale,
    sum_23_sale,
    sum_0_sale,
    sum_1_sale,
    sum_2_sale,
    end_qty
    from mid_sale_add_sum_spark
    where sale_date>=date_sub('{0}', 365) and sale_date<='{0}'
    """.format(item)
    df_tmp1_1 = spark.sql(tmp1_1_sql)
    df_tmp1_1.createOrReplaceTempView("tmp1_1_spark")
    print("tmp1_1")
    #df_tmp1_1.show()
    tmp1_sql = """
    select store_code,
    product_code,
    date_case,
    sale_date,
    max_hour,
    sum_3_sale,
    sum_4_sale,
    sum_5_sale,
    sum_6_sale,
    sum_7_sale,
    sum_8_sale,
    sum_9_sale,
    sum_10_sale,
    sum_11_sale,
    sum_12_sale,
    sum_13_sale,
    sum_14_sale,
    sum_15_sale,
    sum_16_sale,
    sum_17_sale,
    sum_18_sale,
    sum_19_sale,
    sum_20_sale,
    sum_21_sale,
    sum_22_sale,
    sum_23_sale,
    sum_0_sale,
    sum_1_sale,
    sum_2_sale,
    end_qty
    from tmp1_1_spark 
    where date_id<=100
    """

    df_tmp1 = spark.sql(tmp1_sql)
    df_tmp1.createOrReplaceTempView("tmp1_spark")

    tmp2_1_sql = """
    select percentile_approx(sum_3_sale,0.25,9999) low,
    percentile_approx(sum_3_sale,0.75,9999) high, 
    store_code,
    product_code,
    date_case
    from tmp1_spark
    group by store_code,product_code,date_case
    """
    df_tmp2_1 = spark.sql(tmp2_1_sql)
    df_tmp2_1.createOrReplaceTempView("tmp2_1_spark")

    tmp2_sql = """
    select store_code,
    product_code,
    date_case,
    low-1.5*(high-low) low,
    high+1.5*(high-low) high
    from tmp2_1_spark
    """
    df_tmp2 = spark.sql(tmp2_sql)
    df_tmp2.createOrReplaceTempView("tmp2_spark")

    tmp3_sql = """
    select t4.store_code,
    t4.product_code,
    t4.date_case,
    t4.sale_date,
    t4.max_hour,
    t4.sum_3_sale,
    t4.sum_4_sale,
    t4.sum_5_sale,
    t4.sum_6_sale,
    t4.sum_7_sale,
    t4.sum_8_sale,
    t4.sum_9_sale,
    t4.sum_10_sale,
    t4.sum_11_sale,
    t4.sum_12_sale,
    t4.sum_13_sale,
    t4.sum_14_sale,
    t4.sum_15_sale,
    t4.sum_16_sale,
    t4.sum_17_sale,
    t4.sum_18_sale,
    t4.sum_19_sale,
    t4.sum_20_sale,
    t4.sum_21_sale,
    t4.sum_22_sale,
    t4.sum_23_sale,
    t4.sum_0_sale,
    t4.sum_1_sale,
    t4.sum_2_sale,
    end_qty
    from tmp1_spark t4
    left join tmp2_spark t5 on t4.store_code=t5.store_code and t4.product_code=t5.product_code and t4.date_case=t5.date_case
    where t4.sum_3_sale>=t5.low and t4.sum_3_sale<=t5.high
    """
    df_tmp3 = spark.sql(tmp3_sql)
    df_tmp3.createOrReplaceTempView("tmp3_spark")
    print("tmp3")
    #df_tmp3.show()
    tmp4_sql = """
    select store_code,
    product_code,
    date_case,
    sale_date,
    max_hour,
    case when max_hour>=3 and max_hour<4 then sum_3_sale
        else sum_3_sale-sum_4_sale end time_3_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<4 then 0
        when max_hour>=4 and max_hour<5 then sum_4_sale
        else sum_4_sale-sum_5_sale end time_4_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<5 then 0
        when max_hour>=5 and max_hour<6 then sum_5_sale
        else sum_5_sale-sum_6_sale end time_5_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<6 then 0
        when max_hour>=6 and max_hour<7 then sum_6_sale
        else sum_6_sale-sum_7_sale end time_6_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<7 then 0
        when max_hour>=7 and max_hour<8 then sum_7_sale
        else sum_7_sale-sum_8_sale end time_7_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<8 then 0
        when max_hour>=8 and max_hour<9 then sum_8_sale
        else sum_8_sale-sum_9_sale end time_8_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<9 then 0
        when max_hour>=9 and max_hour<10 then sum_9_sale
        else sum_9_sale-sum_10_sale end time_9_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<10 then 0
        when max_hour>=10 and max_hour<11 then sum_10_sale
        else sum_10_sale-sum_11_sale end time_10_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<11 then 0
        when max_hour>=11 and max_hour<12 then sum_11_sale
        else sum_11_sale-sum_12_sale end time_11_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<12 then 0
        when max_hour>=12 and max_hour<13 then sum_12_sale
        else sum_12_sale-sum_13_sale end time_12_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<13 then 0
        when max_hour>=13 and max_hour<14 then sum_13_sale
        else sum_13_sale-sum_14_sale end time_13_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<14 then 0
        when max_hour>=14 and max_hour<15 then sum_14_sale
        else sum_14_sale-sum_15_sale end time_14_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<15 then 0
        when max_hour>=15 and max_hour<16 then sum_15_sale
        else sum_15_sale-sum_16_sale end time_15_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<16 then 0
        when max_hour>=16 and max_hour<17 then sum_16_sale
        else sum_16_sale-sum_17_sale end time_16_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<17 then 0
        when max_hour>=17 and max_hour<18 then sum_17_sale
        else sum_17_sale-sum_18_sale end time_17_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<18 then 0
        when max_hour>=18 and max_hour<19 then sum_18_sale
        else sum_18_sale-sum_19_sale end time_18_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<19 then 0
        when max_hour>=19 and max_hour<20 then sum_19_sale
        else sum_19_sale-sum_20_sale end time_19_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<20 then 0
        when max_hour>=20 and max_hour<21 then sum_20_sale
        else sum_20_sale-sum_21_sale end time_20_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<21 then 0
        when max_hour>=21 and max_hour<22 then sum_21_sale
        else sum_21_sale-sum_22_sale end time_21_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<22 then 0
        when max_hour>=22 and max_hour<23 then sum_22_sale
        else sum_22_sale-sum_23_sale end time_22_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<23 then 0
        when max_hour>=23 and max_hour<24 then sum_23_sale
        else sum_23_sale-sum_0_sale end time_23_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<24 then 0
        when max_hour>=24 and max_hour<25 then sum_0_sale
        else sum_0_sale-sum_1_sale end time_0_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<25 then 0
        when max_hour>=25 and max_hour<26 then sum_1_sale
        else sum_1_sale-sum_2_sale end time_1_sale,
    case when (end_qty<>0 and end_qty is not null) and max_hour<26 then 0
        else sum_2_sale end time_2_sale,
    end_qty
    from tmp3_spark
    """
    df_tmp4 = spark.sql(tmp4_sql)
    df_tmp4.createOrReplaceTempView("tmp4_spark")

    tmp5_1_sql = """
    select row_number() over(partition by store_code,product_code,date_case order by sale_date desc) date_id,
    store_code,
    product_code,
    date_case,
    sale_date,
    max_hour,
    time_3_sale,
    time_4_sale,
    time_5_sale,
    time_6_sale,
    time_7_sale,
    time_8_sale,
    time_9_sale,
    time_10_sale,
    time_11_sale,
    time_12_sale,
    time_13_sale,
    time_14_sale,
    time_15_sale,
    time_16_sale,
    time_17_sale,
    time_18_sale,
    time_19_sale,
    time_20_sale,
    time_21_sale,
    time_22_sale,
    time_23_sale,
    time_0_sale,
    time_1_sale,
    time_2_sale,
    end_qty
    from tmp4_spark 
    """
    df_tmp5_1 = spark.sql(tmp5_1_sql)
    df_tmp5_1.createOrReplaceTempView("tmp5_1_spark")

    tmp5_sql = """
    select 
    store_code,
    product_code,
    date_case,
    sale_date,
    max_hour,
    time_3_sale,
    time_4_sale,
    time_5_sale,
    time_6_sale,
    time_7_sale,
    time_8_sale,
    time_9_sale,
    time_10_sale,
    time_11_sale,
    time_12_sale,
    time_13_sale,
    time_14_sale,
    time_15_sale,
    time_16_sale,
    time_17_sale,
    time_18_sale,
    time_19_sale,
    time_20_sale,
    time_21_sale,
    time_22_sale,
    time_23_sale,
    time_0_sale,
    time_1_sale,
    time_2_sale,
    end_qty
    from tmp5_1_spark 
    where date_id<=30
    """
    df_tmp5 = spark.sql(tmp5_sql)
    df_tmp5.createOrReplaceTempView("tmp5_spark")

    sql_test = """
    select store_code,
    product_code,
    date_case,
    coalesce(avg(time_3_sale),0) avg_3_sale,
    coalesce(avg(time_4_sale),0) avg_4_sale,
    coalesce(avg(time_5_sale),0) avg_5_sale,
    coalesce(avg(time_6_sale),0) avg_6_sale,
    coalesce(avg(time_7_sale),0) avg_7_sale,
    coalesce(avg(time_8_sale),0) avg_8_sale,
    coalesce(avg(time_9_sale),0) avg_9_sale,
    coalesce(avg(time_10_sale),0) avg_10_sale,
    coalesce(avg(time_11_sale),0) avg_11_sale,
    coalesce(avg(time_12_sale),0) avg_12_sale,
    coalesce(avg(time_13_sale),0) avg_13_sale,
    coalesce(avg(time_14_sale),0) avg_14_sale,
    coalesce(avg(time_15_sale),0) avg_15_sale,
    coalesce(avg(time_16_sale),0) avg_16_sale,
    coalesce(avg(time_17_sale),0) avg_17_sale,
    coalesce(avg(time_18_sale),0) avg_18_sale,
    coalesce(avg(time_19_sale),0) avg_19_sale,
    coalesce(avg(time_20_sale),0) avg_20_sale,
    coalesce(avg(time_21_sale),0) avg_21_sale,
    coalesce(avg(time_22_sale),0) avg_22_sale,
    coalesce(avg(time_23_sale),0) avg_23_sale,
    coalesce(avg(time_0_sale),0)  avg_0_sale,
    coalesce(avg(time_1_sale),0) avg_1_sale,
    coalesce(avg(time_2_sale),0) avg_2_sale
    from tmp5_spark
    group by store_code,
    product_code,
    date_case
    """
    df_test = spark.sql(sql_test)
    df_test.createOrReplaceTempView("tmp_test")

    insert_sql = """
    insert into table rbu_sxcp_edw_ai_dev.sale_add_avg
    (
    select 
    store_code,
    product_code,
    '{0}' sale_date,
    date_case,
    avg_3_sale,
    avg_4_sale,
    avg_5_sale,
    avg_6_sale,
    avg_7_sale,
    avg_8_sale,
    avg_9_sale,
    avg_10_sale,
    avg_11_sale,
    avg_12_sale,
    avg_13_sale,
    avg_14_sale,
    avg_15_sale,
    avg_16_sale,
    avg_17_sale,
    avg_18_sale,
    avg_19_sale,
    avg_20_sale,
    avg_21_sale,
    avg_22_sale,
    avg_23_sale,
    avg_0_sale,
    avg_1_sale,
    avg_2_sale,
    current_timestamp()
    from tmp_test)
    """.format(item)
    spark.sql(insert_sql)
    print("插入数据成功")
    df_tmp1_1.drop()
    df_tmp1.drop()
    df_tmp2_1.drop()
    df_tmp2.drop()
    df_tmp3.drop()
    df_tmp4.drop()
    df_tmp5.drop()
    df_tmp5_1.drop()
    df_mid_sale_add_sum.drop()
    print("删除临时表成功")


for i in range(0, 30):
    min_day = datetime.datetime.strptime('2018-11-01', "%Y-%m-%d") + datetime.timedelta(days=i)
    jiaoben(str(min_day)[:10])
    print(str(min_day)[:10])

print("process successfully!")
print("=====================")
print("到此结束")

