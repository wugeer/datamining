from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, UDFRegistration, SparkSession
from pyspark.sql.functions import udf
import datetime
import re
import time

spark = SparkSession.builder.master("yarn").appName("mid_turnover_feat").enableHiveSupport().getOrCreate()

print("starting..........")
mon = ['2019-01-01', '2019-01-04']
sql_fct_store_product_on_sale = """
select * from rbu_sxcp_edw_dev.fct_store_product_on_sale_history 
where sale_date>='{0}' and sale_date<'{1}'
""".format(mon[0], mon[1])
df_fct_store_product_on_sale = spark.sql(sql_fct_store_product_on_sale)
df_fct_store_product_on_sale.createOrReplaceTempView("tmp_fct_store_product_on_sale")
spark.catalog.cacheTable("tmp_fct_store_product_on_sale")

sql_rst_weather = """
select * from rbu_sxcp_ods_dev.rst_weather
"""
df_rst_weather = spark.sql(sql_rst_weather)
df_rst_weather.createOrReplaceTempView("tmp_rst_weather")
spark.catalog.cacheTable("tmp_rst_weather")

sql_dim_store = """
select * 
from rbu_sxcp_edw_dev.dim_store
where store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
"""
df_dim_store = spark.sql(sql_dim_store)
df_dim_store.createOrReplaceTempView("tmp_dim_store")
spark.catalog.cacheTable("tmp_dim_store")

sql_store_category = """
select shop_code store_code,date sale_date,case date_case
from rbu_sxcp_ods_dev.store_category
where shop_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
"""
df_store_category = spark.sql(sql_store_category)
df_store_category.createOrReplaceTempView("tmp_store_category")
spark.catalog.cacheTable("tmp_store_category")

sql_fct_store_sale_add = """
select  store_code,product_code,check_date,sale_total_qty
from rbu_sxcp_edw_ai_dev.store_sale_add
where check_date<'{0}'
""".format(mon[1])
df_fct_store_sale_add_1 = spark.sql(sql_fct_store_sale_add)
# 去重
df_fct_store_sale_add_1 = df_fct_store_sale_add_1.dropDuplicates()
df_fct_store_sale_add_1.createOrReplaceTempView("tmp_fct_store_sale_add")
spark.catalog.cacheTable("tmp_fct_store_sale_add")

sql_dim_store_product = """
select * from rbu_sxcp_edw_dev.dim_store_product
where store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
"""
df_dim_store_product = spark.sql(sql_dim_store_product)
df_dim_store_product.createOrReplaceTempView("tmp_dim_store_product")
spark.catalog.cacheTable("tmp_dim_store_product")

sql_fct_pos_detail = """
select * from rbu_sxcp_edw_dev.fct_pos_detail
where store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
"""
df_fct_pos_detail = spark.sql(sql_fct_pos_detail)
df_fct_pos_detail.createOrReplaceTempView("tmp_fct_pos_detail")
spark.catalog.cacheTable("tmp_fct_pos_detail")

tmp3_sql = """
select 
store_code,
min(sale_date) min_date
from tmp_fct_pos_detail
group by store_code
"""
df_tmp3 = spark.sql(tmp3_sql)
df_tmp3.createOrReplaceTempView("tmp3_spark")
spark.catalog.cacheTable("tmp3_spark")


def jiaoben(item1, item2):
    tmp1_sql = """
    select distinct store_code from tmp_fct_store_product_on_sale where sale_date='{0}'
    """.format(item2)
    df_tmp1 = spark.sql(tmp1_sql)
    df_tmp1.createOrReplaceTempView("tmp1_spark")

    tmp2_sql = """
    select t8.store_code,
    t9.sale_date,
    t9.date_case,
    sum(t10.sale_total_qty*COALESCE(t11.sale_price,0)) turnover
    from tmp1_spark t8
    left join tmp_store_category t9 on t8.store_code=t9.store_code
    left join tmp_fct_store_sale_add t10 on t9.store_code=t10.store_code and t9.sale_date=t10.check_date
    left join tmp_dim_store_product t11 on t10.store_code=t11.store_code and t10.product_code=t11.product_code
    where t9.sale_date>='2017-07-01' 
    group by t8.store_code,t9.sale_date,t9.date_case
    """
    df_tmp2 = spark.sql(tmp2_sql)
    df_tmp2.createOrReplaceTempView("tmp2_spark")

    tmp_mid_turnover_feat_sql = """
    select t1.store_code,
    t1.sale_date,
    t1.date_case,
    t1.turnover
    from tmp2_spark t1
    left join tmp3_spark t2 on t1.store_code=t2.store_code
    where t1.sale_date>=t2.min_date and coalesce(t1.turnover,0)>=0
    """
    df_tmp_mid_turnover_feat_sql = spark.sql(tmp_mid_turnover_feat_sql)
    df_tmp_mid_turnover_feat_sql.createOrReplaceTempView("tmp_mid_turnover_feat_sql_spark")
    spark.catalog.cacheTable("tmp_mid_turnover_feat_sql_spark")

    tmp4_sql = """
    select 
    t1.store_code,
    t1.sale_date,
    t2.sale_hour,
    t2.qty*coalesce(t3.sale_price,0) amt
    from tmp_store_category t1
    inner join tmp1_spark t0 on t1.store_code=t0.store_code
    left join tmp_fct_pos_detail t2 on t1.store_code=t2.store_code and t1.sale_date=t2.sale_date
    left join tmp_dim_store_product t3 on t2.store_code=t3.store_code and t2.product_code=t3.product_code 
    """
    df_tmp4 = spark.sql(tmp4_sql)
    df_tmp4.createOrReplaceTempView("tmp4_spark")

    tmp5_sql = """
    select 
    store_code,
    sale_date,
    sale_hour,
    sum(coalesce(amt,0)) turnover
    from tmp4_spark
    group by store_code,sale_date,sale_hour
    """
    df_tmp5 = spark.sql(tmp5_sql)
    df_tmp5.createOrReplaceTempView("tmp5_spark")
    df_tmp4.drop()

    tmp6_sql = """
    select 
    store_code,
    sale_date,
    coalesce(sum(case when sale_hour>=3 then turnover end),0) sum_3_turnover,
    coalesce(sum(case when sale_hour>=4 then turnover end),0) sum_4_turnover,
    coalesce(sum(case when sale_hour>=5 then turnover end),0) sum_5_turnover,
    coalesce(sum(case when sale_hour>=6 then turnover end),0) sum_6_turnover,
    coalesce(sum(case when sale_hour>=7 then turnover end),0) sum_7_turnover,
    coalesce(sum(case when sale_hour>=8 then turnover end),0) sum_8_turnover,
    coalesce(sum(case when sale_hour>=9 then turnover end),0) sum_9_turnover,
    coalesce(sum(case when sale_hour>=10 then turnover end),0) sum_10_turnover,
    coalesce(sum(case when sale_hour>=11 then turnover end),0) sum_11_turnover,
    coalesce(sum(case when sale_hour>=12 then turnover end),0) sum_12_turnover,
    coalesce(sum(case when sale_hour>=13 then turnover end),0) sum_13_turnover,
    coalesce(sum(case when sale_hour>=14 then turnover end),0) sum_14_turnover,
    coalesce(sum(case when sale_hour>=15 then turnover end),0) sum_15_turnover,
    coalesce(sum(case when sale_hour>=16 then turnover end),0) sum_16_turnover,
    coalesce(sum(case when sale_hour>=17 then turnover end),0) sum_17_turnover,
    coalesce(sum(case when sale_hour>=18 then turnover end),0) sum_18_turnover,
    coalesce(sum(case when sale_hour>=19 then turnover end),0) sum_19_turnover,
    coalesce(sum(case when sale_hour>=20 then turnover end),0) sum_20_turnover,
    coalesce(sum(case when sale_hour>=21 then turnover end),0) sum_21_turnover,
    coalesce(sum(case when sale_hour>=22 then turnover end),0) sum_22_turnover,
    coalesce(sum(case when sale_hour>=23 then turnover end),0) sum_23_turnover,
    coalesce(sum(case when sale_hour>=24 then turnover end),0) sum_0_turnover,
    coalesce(sum(case when sale_hour>=25 then turnover end),0) sum_1_turnover,
    coalesce(sum(case when sale_hour>=26 then turnover end),0) sum_2_turnover
    from tmp5_spark
    group by store_code,sale_date
    """
    df_tmp6 = spark.sql(tmp6_sql)
    df_tmp6.createOrReplaceTempView("tmp6_spark")
    df_tmp5.drop()

    tmp_turnover_feat_sum_sql = """
    select 
    store_code,
    sale_date,
    sum_3_turnover-sum_4_turnover time_3_turnover,
    sum_4_turnover-sum_5_turnover time_4_turnover,
    sum_5_turnover-sum_6_turnover time_5_turnover,
    sum_6_turnover-sum_7_turnover time_6_turnover,
    sum_7_turnover-sum_8_turnover time_7_turnover,
    sum_8_turnover-sum_9_turnover time_8_turnover,
    sum_9_turnover-sum_10_turnover time_9_turnover,
    sum_10_turnover-sum_11_turnover time_10_turnover,
    sum_11_turnover-sum_12_turnover time_11_turnover,
    sum_12_turnover-sum_13_turnover time_12_turnover,
    sum_13_turnover-sum_14_turnover time_13_turnover,
    sum_14_turnover-sum_15_turnover time_14_turnover,
    sum_15_turnover-sum_16_turnover time_15_turnover,
    sum_16_turnover-sum_17_turnover time_16_turnover,
    sum_17_turnover-sum_18_turnover time_17_turnover,
    sum_18_turnover-sum_19_turnover time_18_turnover,
    sum_19_turnover-sum_20_turnover time_19_turnover,
    sum_20_turnover-sum_21_turnover time_20_turnover,
    sum_21_turnover-sum_22_turnover time_21_turnover,
    sum_22_turnover-sum_23_turnover time_22_turnover,
    sum_23_turnover-sum_0_turnover time_23_turnover,
    sum_0_turnover-sum_1_turnover time_0_turnover,
    sum_1_turnover-sum_2_turnover time_1_turnover,
    sum_2_turnover time_2_turnover
    from tmp6_spark
    """
    df_tmp_turnover_feat_sum = spark.sql(tmp_turnover_feat_sum_sql)
    df_tmp_turnover_feat_sum.createOrReplaceTempView("tmp_turnover_feat_sum_spark")
    spark.catalog.cacheTable("tmp_turnover_feat_sum_spark")

    df_tmp6.drop()
    print("取%s一年的值,求合理值范围" % item2)
    sql_tmp_yichangzhi = """
    select 
    percentile_approx(time_3_turnover,0.25,9999)-3*(percentile_approx(time_3_turnover,0.75,9999)-percentile_approx(time_3_turnover,0.25,9999)) low_3,
    percentile_approx(time_3_turnover,0.75,9999)+3*(percentile_approx(time_3_turnover,0.75,9999)-percentile_approx(time_3_turnover,0.25,9999)) high_3,
    percentile_approx(time_4_turnover,0.25,9999)-3*(percentile_approx(time_4_turnover,0.75,9999)-percentile_approx(time_4_turnover,0.25,9999)) low_4,
    percentile_approx(time_4_turnover,0.75,9999)+3*(percentile_approx(time_4_turnover,0.75,9999)-percentile_approx(time_4_turnover,0.25,9999)) high_4,
    percentile_approx(time_5_turnover,0.25,9999)-3*(percentile_approx(time_5_turnover,0.75,9999)-percentile_approx(time_5_turnover,0.25,9999)) low_5,
    percentile_approx(time_5_turnover,0.75,9999)+3*(percentile_approx(time_5_turnover,0.75,9999)-percentile_approx(time_5_turnover,0.25,9999)) high_5,
    percentile_approx(time_6_turnover,0.25,9999)-3*(percentile_approx(time_6_turnover,0.75,9999)-percentile_approx(time_6_turnover,0.25,9999)) low_6,
    percentile_approx(time_6_turnover,0.75,9999)+3*(percentile_approx(time_6_turnover,0.75,9999)-percentile_approx(time_6_turnover,0.25,9999)) high_6,
    percentile_approx(time_7_turnover,0.25,9999)-3*(percentile_approx(time_7_turnover,0.75,9999)-percentile_approx(time_7_turnover,0.25,9999)) low_7,
    percentile_approx(time_7_turnover,0.75,9999)+3*(percentile_approx(time_7_turnover,0.75,9999)-percentile_approx(time_7_turnover,0.25,9999)) high_7,
    percentile_approx(time_8_turnover,0.25,9999)-3*(percentile_approx(time_8_turnover,0.75,9999)-percentile_approx(time_8_turnover,0.25,9999)) low_8,
    percentile_approx(time_8_turnover,0.75,9999)+3*(percentile_approx(time_8_turnover,0.75,9999)-percentile_approx(time_8_turnover,0.25,9999)) high_8,
    percentile_approx(time_9_turnover,0.25,9999)-3*(percentile_approx(time_9_turnover,0.75,9999)-percentile_approx(time_9_turnover,0.25,9999)) low_9,
    percentile_approx(time_9_turnover,0.75,9999)+3*(percentile_approx(time_9_turnover,0.75,9999)-percentile_approx(time_9_turnover,0.25,9999)) high_9,
    percentile_approx(time_10_turnover,0.25,9999)-3*(percentile_approx(time_10_turnover,0.75,9999)-percentile_approx(time_10_turnover,0.25,9999)) low_10,
    percentile_approx(time_10_turnover,0.75,9999)+3*(percentile_approx(time_10_turnover,0.75,9999)-percentile_approx(time_10_turnover,0.25,9999)) high_10,
    percentile_approx(time_11_turnover,0.25,9999)-3*(percentile_approx(time_11_turnover,0.75,9999)-percentile_approx(time_11_turnover,0.25,9999)) low_11,
    percentile_approx(time_11_turnover,0.75,9999)+3*(percentile_approx(time_11_turnover,0.75,9999)-percentile_approx(time_11_turnover,0.25,9999)) high_11,
    percentile_approx(time_12_turnover,0.25,9999)-3*(percentile_approx(time_12_turnover,0.75,9999)-percentile_approx(time_12_turnover,0.25,9999)) low_12,
    percentile_approx(time_12_turnover,0.75,9999)+3*(percentile_approx(time_12_turnover,0.75,9999)-percentile_approx(time_12_turnover,0.25,9999)) high_12,
    percentile_approx(time_13_turnover,0.25,9999)-3*(percentile_approx(time_13_turnover,0.75,9999)-percentile_approx(time_13_turnover,0.25,9999)) low_13,
    percentile_approx(time_13_turnover,0.75,9999)+3*(percentile_approx(time_13_turnover,0.75,9999)-percentile_approx(time_13_turnover,0.25,9999)) high_13,
    percentile_approx(time_14_turnover,0.25,9999)-3*(percentile_approx(time_14_turnover,0.75,9999)-percentile_approx(time_14_turnover,0.25,9999)) low_14,
    percentile_approx(time_14_turnover,0.75,9999)+3*(percentile_approx(time_14_turnover,0.75,9999)-percentile_approx(time_14_turnover,0.25,9999)) high_14,
    percentile_approx(time_15_turnover,0.25,9999)-3*(percentile_approx(time_15_turnover,0.75,9999)-percentile_approx(time_15_turnover,0.25,9999)) low_15,
    percentile_approx(time_15_turnover,0.75,9999)+3*(percentile_approx(time_15_turnover,0.75,9999)-percentile_approx(time_15_turnover,0.25,9999)) high_15,
    percentile_approx(time_16_turnover,0.25,9999)-3*(percentile_approx(time_16_turnover,0.75,9999)-percentile_approx(time_16_turnover,0.25,9999)) low_16,
    percentile_approx(time_16_turnover,0.75,9999)+3*(percentile_approx(time_16_turnover,0.75,9999)-percentile_approx(time_16_turnover,0.25,9999)) high_16,
    percentile_approx(time_17_turnover,0.25,9999)-3*(percentile_approx(time_17_turnover,0.75,9999)-percentile_approx(time_17_turnover,0.25,9999)) low_17,
    percentile_approx(time_17_turnover,0.75,9999)+3*(percentile_approx(time_17_turnover,0.75,9999)-percentile_approx(time_17_turnover,0.25,9999)) high_17,
    percentile_approx(time_18_turnover,0.25,9999)-3*(percentile_approx(time_18_turnover,0.75,9999)-percentile_approx(time_18_turnover,0.25,9999)) low_18,
    percentile_approx(time_18_turnover,0.75,9999)+3*(percentile_approx(time_18_turnover,0.75,9999)-percentile_approx(time_18_turnover,0.25,9999)) high_18,
    percentile_approx(time_19_turnover,0.25,9999)-3*(percentile_approx(time_19_turnover,0.75,9999)-percentile_approx(time_19_turnover,0.25,9999)) low_19,
    percentile_approx(time_19_turnover,0.75,9999)+3*(percentile_approx(time_19_turnover,0.75,9999)-percentile_approx(time_19_turnover,0.25,9999)) high_19,
    percentile_approx(time_20_turnover,0.25,9999)-3*(percentile_approx(time_20_turnover,0.75,9999)-percentile_approx(time_20_turnover,0.25,9999)) low_20,
    percentile_approx(time_20_turnover,0.75,9999)+3*(percentile_approx(time_20_turnover,0.75,9999)-percentile_approx(time_20_turnover,0.25,9999)) high_20,
    percentile_approx(time_21_turnover,0.25,9999)-3*(percentile_approx(time_21_turnover,0.75,9999)-percentile_approx(time_21_turnover,0.25,9999)) low_21,
    percentile_approx(time_21_turnover,0.75,9999)+3*(percentile_approx(time_21_turnover,0.75,9999)-percentile_approx(time_21_turnover,0.25,9999)) high_21,
    percentile_approx(time_22_turnover,0.25,9999)-3*(percentile_approx(time_22_turnover,0.75,9999)-percentile_approx(time_22_turnover,0.25,9999)) low_22,
    percentile_approx(time_22_turnover,0.75,9999)+3*(percentile_approx(time_22_turnover,0.75,9999)-percentile_approx(time_22_turnover,0.25,9999)) high_22,
    percentile_approx(time_23_turnover,0.25,9999)-3*(percentile_approx(time_23_turnover,0.75,9999)-percentile_approx(time_23_turnover,0.25,9999)) low_23,
    percentile_approx(time_23_turnover,0.75,9999)+3*(percentile_approx(time_23_turnover,0.75,9999)-percentile_approx(time_23_turnover,0.25,9999)) high_23,
    percentile_approx(time_0_turnover,0.25,9999)-3*(percentile_approx(time_0_turnover,0.75,9999)-percentile_approx(time_0_turnover,0.25,9999)) low_24,
    percentile_approx(time_0_turnover,0.75,9999)+3*(percentile_approx(time_0_turnover,0.75,9999)-percentile_approx(time_0_turnover,0.25,9999)) high_24,
    percentile_approx(time_1_turnover,0.25,9999)-3*(percentile_approx(time_1_turnover,0.75,9999)-percentile_approx(time_1_turnover,0.25,9999)) low_25,
    percentile_approx(time_1_turnover,0.75,9999)+3*(percentile_approx(time_1_turnover,0.75,9999)-percentile_approx(time_1_turnover,0.25,9999)) high_25,
    percentile_approx(time_2_turnover,0.25,9999)-3*(percentile_approx(time_2_turnover,0.75,9999)-percentile_approx(time_2_turnover,0.25,9999)) low_26,
    percentile_approx(time_2_turnover,0.75,9999)+3*(percentile_approx(time_2_turnover,0.75,9999)-percentile_approx(time_2_turnover,0.25,9999)) high_26,
    store_code
    from tmp_turnover_feat_sum_spark
    where sale_date>='{0}' and sale_date<='{1}'
    group by store_code
    """.format(item1, item2)
    df_tmp_yichangzhi = spark.sql(sql_tmp_yichangzhi)
    df_tmp_yichangzhi.createOrReplaceTempView("tmp_yichangzhi_spark")

    print("剔除一年内数据的异常值")
    tmp7_sql = """
    select 
    t4.store_code,
    t4.sale_date,
    case when t4.time_3_turnover>=low_3 and t4.time_3_turnover<=high_3 then t4.time_3_turnover
        else null 
    end time_3_turnover,
    case when t4.time_4_turnover>=low_4 and t4.time_4_turnover<=high_4 then t4.time_4_turnover
        else null 
    end time_4_turnover,
    case when t4.time_5_turnover>=low_5 and t4.time_5_turnover<=high_5 then t4.time_5_turnover
        else null 
    end time_5_turnover,
    case when t4.time_6_turnover>=low_6 and t4.time_6_turnover<=high_6 then t4.time_6_turnover
        else null 
    end time_6_turnover,
    case when t4.time_7_turnover>=low_7 and t4.time_7_turnover<=high_7 then t4.time_7_turnover
        else null 
    end time_7_turnover,
    case when t4.time_8_turnover>=low_8 and t4.time_8_turnover<=high_8 then t4.time_8_turnover
        else null 
    end time_8_turnover,
    case when t4.time_9_turnover>=low_9 and t4.time_9_turnover<=high_9 then t4.time_9_turnover
        else null 
    end time_9_turnover,
    case when t4.time_10_turnover>=low_10 and t4.time_10_turnover<=high_10 then t4.time_10_turnover
        else null 
    end time_10_turnover,
    case when t4.time_11_turnover>=low_11 and t4.time_11_turnover<=high_11 then t4.time_11_turnover
        else null 
    end time_11_turnover,
    case when t4.time_12_turnover>=low_12 and t4.time_12_turnover<=high_12 then t4.time_12_turnover
        else null 
    end time_12_turnover,
    case when t4.time_13_turnover>=low_13 and t4.time_13_turnover<=high_13 then t4.time_13_turnover
        else null 
    end time_13_turnover,
    case when t4.time_14_turnover>=low_14 and t4.time_14_turnover<=high_14 then t4.time_14_turnover
        else null 
    end time_14_turnover,
    case when t4.time_15_turnover>=low_15 and t4.time_15_turnover<=high_15 then t4.time_15_turnover
        else null 
    end time_15_turnover,
    case when t4.time_16_turnover>=low_16 and t4.time_16_turnover<=high_16 then t4.time_16_turnover
        else null 
    end time_16_turnover,
    case when t4.time_17_turnover>=low_17 and t4.time_17_turnover<=high_17 then t4.time_17_turnover
        else null 
    end time_17_turnover,
    case when t4.time_18_turnover>=low_18 and t4.time_18_turnover<=high_18 then t4.time_18_turnover
        else null 
    end time_18_turnover,
    case when t4.time_19_turnover>=low_19 and t4.time_19_turnover<=high_19 then t4.time_19_turnover
        else null 
    end time_19_turnover,
    case when t4.time_20_turnover>=low_20 and t4.time_20_turnover<=high_20 then t4.time_20_turnover
        else null 
    end time_20_turnover,
    case when t4.time_21_turnover>=low_21 and t4.time_21_turnover<=high_21 then t4.time_21_turnover
        else null 
    end time_21_turnover,
    case when t4.time_22_turnover>=low_22 and t4.time_22_turnover<=high_22 then t4.time_22_turnover
        else null 
    end time_22_turnover,
    case when t4.time_23_turnover>=low_23 and t4.time_23_turnover<=high_23 then t4.time_23_turnover
        else null 
    end time_23_turnover,
    case when t4.time_0_turnover>=low_24 and t4.time_0_turnover<=high_24 then t4.time_0_turnover
        else null 
    end time_0_turnover,
    case when t4.time_1_turnover>=low_25 and t4.time_1_turnover<=high_25 then t4.time_1_turnover
        else null 
    end time_1_turnover,
    case when t4.time_2_turnover>=low_26 and t4.time_2_turnover<=high_26 then t4.time_2_turnover
        else null 
    end time_2_turnover
    from tmp_turnover_feat_sum_spark t4
    left join tmp_yichangzhi_spark on t4.store_code=tmp_yichangzhi_spark.store_code
    where t4.sale_date>='{0}' and t4.sale_date<='{1}'
    """.format(item1, item2)
    df_tmp7 = spark.sql(tmp7_sql)
    df_tmp7.createOrReplaceTempView("tmp7_spark")
    df_tmp_yichangzhi.drop()
    df_tmp_turnover_feat_sum.drop()
    tmp31_sql = """
    select 
    store_code,
    avg(time_3_turnover) avg_3_turnover,
    avg(time_4_turnover) avg_4_turnover,
    avg(time_5_turnover) avg_5_turnover,
    avg(time_6_turnover) avg_6_turnover,
    avg(time_7_turnover) avg_7_turnover,
    avg(time_8_turnover) avg_8_turnover,
    avg(time_9_turnover) avg_9_turnover,
    avg(time_10_turnover) avg_10_turnover,
    avg(time_11_turnover) avg_11_turnover,
    avg(time_12_turnover) avg_12_turnover,
    avg(time_13_turnover) avg_13_turnover,
    avg(time_14_turnover) avg_14_turnover,
    avg(time_15_turnover) avg_15_turnover,
    avg(time_16_turnover) avg_16_turnover,
    avg(time_17_turnover) avg_17_turnover,
    avg(time_18_turnover) avg_18_turnover,
    avg(time_19_turnover) avg_19_turnover,
    avg(time_20_turnover) avg_20_turnover,
    avg(time_21_turnover) avg_21_turnover,
    avg(time_22_turnover) avg_22_turnover,
    avg(time_23_turnover) avg_23_turnover,
    avg(time_0_turnover) avg_0_turnover,
    avg(time_1_turnover) avg_1_turnover,
    avg(time_2_turnover) avg_2_turnover,
    case when avg(time_3_turnover)+avg(time_4_turnover)+avg(time_5_turnover)+avg(time_6_turnover)+avg(time_7_turnover)+avg(time_8_turnover)+avg(time_9_turnover)+avg(time_10_turnover)+avg(time_11_turnover)+avg(time_12_turnover)+avg(time_13_turnover)+avg(time_14_turnover)+avg(time_15_turnover)+avg(time_16_turnover)+avg(time_17_turnover)+avg(time_18_turnover)+avg(time_19_turnover)+avg(time_20_turnover)+avg(time_21_turnover)+avg(time_22_turnover)+avg(time_23_turnover)+avg(time_0_turnover)+avg(time_1_turnover)+avg(time_2_turnover)=0 then 1 
        else avg(time_3_turnover)+avg(time_4_turnover)+avg(time_5_turnover)+avg(time_6_turnover)+avg(time_7_turnover)+avg(time_8_turnover)+avg(time_9_turnover)+avg(time_10_turnover)+avg(time_11_turnover)+avg(time_12_turnover)+avg(time_13_turnover)+avg(time_14_turnover)+avg(time_15_turnover)+avg(time_16_turnover)+avg(time_17_turnover)+avg(time_18_turnover)+avg(time_19_turnover)+avg(time_20_turnover)+avg(time_21_turnover)+avg(time_22_turnover)+avg(time_23_turnover)+avg(time_0_turnover)+avg(time_1_turnover)+avg(time_2_turnover) 
    end sum_avg
    from tmp7_spark
    group by store_code
    """
    df_tmp31 = spark.sql(tmp31_sql)
    df_tmp31.createOrReplaceTempView("tmp31_spark")
    df_tmp7.drop()

    sql_mid_turnover_feat_avg = """
    select 
    store_code,
    avg_3_turnover/sum_avg per_3_turnover,
    avg_4_turnover/sum_avg per_4_turnover,
    avg_5_turnover/sum_avg per_5_turnover,
    avg_6_turnover/sum_avg per_6_turnover,
    avg_7_turnover/sum_avg per_7_turnover,
    avg_8_turnover/sum_avg per_8_turnover,
    avg_9_turnover/sum_avg per_9_turnover,
    avg_10_turnover/sum_avg per_10_turnover,
    avg_11_turnover/sum_avg per_11_turnover,
    avg_12_turnover/sum_avg per_12_turnover,
    avg_13_turnover/sum_avg per_13_turnover,
    avg_14_turnover/sum_avg per_14_turnover,
    avg_15_turnover/sum_avg per_15_turnover,
    avg_16_turnover/sum_avg per_16_turnover,
    avg_17_turnover/sum_avg per_17_turnover,
    avg_18_turnover/sum_avg per_18_turnover,
    avg_19_turnover/sum_avg per_19_turnover,
    avg_20_turnover/sum_avg per_20_turnover,
    avg_21_turnover/sum_avg per_21_turnover,
    avg_22_turnover/sum_avg per_22_turnover,
    avg_23_turnover/sum_avg per_23_turnover,
    avg_0_turnover/sum_avg per_0_turnover,
    avg_1_turnover/sum_avg per_1_turnover,
    avg_2_turnover/sum_avg per_2_turnover
    from tmp31_spark 
    """
    df_mid_turnover_feat_avg = spark.sql(sql_mid_turnover_feat_avg)
    df_mid_turnover_feat_avg.createOrReplaceTempView("tmp_mid_turnover_feat_avg")

    df_tmp31.drop()
    tmp32_sql = """
    select 
    t5.store_code,
    t7.temp_3*t5.per_3_turnover+t7.temp_4*t5.per_4_turnover+t7.temp_5*t5.per_5_turnover+t7.temp_6*t5.per_6_turnover+t7.temp_7*t5.per_7_turnover+t7.temp_8*t5.per_8_turnover+t7.temp_9*t5.per_9_turnover+t7.temp_10*t5.per_10_turnover+t7.temp_11*t5.per_11_turnover+t7.temp_12*t5.per_12_turnover+t7.temp_13*t5.per_13_turnover+t7.temp_14*t5.per_14_turnover+t7.temp_15*t5.per_15_turnover+t7.temp_16*t5.per_16_turnover+t7.temp_17*t5.per_17_turnover+t7.temp_18*t5.per_18_turnover+t7.temp_19*t5.per_19_turnover+t7.temp_20*t5.per_20_turnover+t7.temp_21*t5.per_21_turnover+t7.temp_22*t5.per_22_turnover+t7.temp_23*t5.per_23_turnover+t7.temp_0*t5.per_0_turnover+t7.temp_1*t5.per_1_turnover+t7.temp_2*t5.per_2_turnover temp_turnover,
    t7.pre_3*t5.per_3_turnover+t7.pre_4*t5.per_4_turnover+t7.pre_5*t5.per_5_turnover+t7.pre_6*t5.per_6_turnover+t7.pre_7*t5.per_7_turnover+t7.pre_8*t5.per_8_turnover+t7.pre_9*t5.per_9_turnover+t7.pre_10*t5.per_10_turnover+t7.pre_11*t5.per_11_turnover+t7.pre_12*t5.per_12_turnover+t7.pre_13*t5.per_13_turnover+t7.pre_14*t5.per_14_turnover+t7.pre_15*t5.per_15_turnover+t7.pre_16*t5.per_16_turnover+t7.pre_17*t5.per_17_turnover+t7.pre_18*t5.per_18_turnover+t7.pre_19*t5.per_19_turnover+t7.pre_20*t5.per_20_turnover+t7.pre_21*t5.per_21_turnover+t7.pre_22*t5.per_22_turnover+t7.pre_23*t5.per_23_turnover+t7.pre_0*t5.per_0_turnover+t7.pre_1*t5.per_1_turnover+t7.pre_2*t5.per_2_turnover pre_turnover
    from tmp_mid_turnover_feat_avg t5
    left join tmp_dim_store t6 on t5.store_code=t6.store_code 
    left join tmp_rst_weather t7 on t6.city=t7.city and t7.weather_date='{0}'
    """.format(item2)
    df_tmp32 = spark.sql(tmp32_sql)
    df_tmp32.createOrReplaceTempView("tmp32_spark")
    df_mid_turnover_feat_avg.drop()
    df_rst_weather.drop()
    df_dim_store.drop()

    tmp33_sql = """
    select 
    row_number() over(partition by store_code,date_case order by sale_date desc) date_id,
    store_code,
    sale_date,
    date_case,
    turnover
    from tmp_mid_turnover_feat_sql_spark
    where sale_date<'{0}' and turnover is not null
    """.format(item2)
    df_tmp33 = spark.sql(tmp33_sql)
    df_tmp33.createOrReplaceTempView("tmp33_spark")
    tmp34_sql = """
    select 
    store_code,
    sale_date,
    date_case,
    turnover
    from tmp33_spark
    where date_id<=30
    """
    df_tmp34 = spark.sql(tmp34_sql)
    df_tmp34.createOrReplaceTempView("tmp34_spark")
    spark.catalog.cacheTable("tmp34_spark")
    df_tmp33.drop()

    tmp35_1_sql = """
    select 
    percentile_approx(turnover,0.25,9999)-3*(percentile_approx(turnover,0.75,9999)-percentile_approx(turnover,0.25,9999)) low,
    percentile_approx(turnover,0.75,9999)+3*(percentile_approx(turnover,0.75,9999)-percentile_approx(turnover,0.25,9999)) high,
    store_code,
    date_case
    from tmp34_spark
    group by store_code,date_case
    """
    df_tmp35_1 = spark.sql(tmp35_1_sql)
    df_tmp35_1.createOrReplaceTempView("tmp35_1_spark")

    tmp35_sql = """
    select 
    d.store_code,
    d.sale_date,
    d.date_case,
    d.turnover
    from tmp34_spark d 
    left join tmp35_1_spark f on d.store_code=f.store_code and d.date_case=f.date_case
    where d.turnover>=f.low and d.turnover<=f.high
    """
    df_tmp35 = spark.sql(tmp35_sql)
    df_tmp35.createOrReplaceTempView("tmp35_spark")
    spark.catalog.uncacheTable("tmp34_spark")

    tmp36_1_sql = """
    select 
    row_number() over(partition by store_code,date_case order by sale_date desc) date_id,
    store_code,
    sale_date,
    date_case,
    turnover
    from tmp35_spark
    where sale_date<'{0}'
    """.format(item2)
    df_tmp36_1 = spark.sql(tmp36_1_sql)
    df_tmp36_1.createOrReplaceTempView("tmp36_1_spark")
    spark.catalog.cacheTable("tmp36_1_spark")

    tmp36_sql = """
    select 
    store_code,
    date_case,
    avg(turnover) recent_3
    from tmp36_1_spark
    where date_id<=3
    group by store_code,date_case
    """
    df_tmp36 = spark.sql(tmp36_sql)
    df_tmp36.createOrReplaceTempView("tmp36_spark")
    df_tmp34.drop()
    df_tmp35.drop()
    df_tmp35_1.drop()

    tmp37_sql = """
    select 
    store_code,
    date_case,
    avg(turnover) recent_7
    from tmp36_1_spark
    where date_id<=7
    group by store_code,date_case
    """
    # 按道理说,recent_3,recent_7不应该有null
    df_tmp37 = spark.sql(tmp37_sql)
    df_tmp37.createOrReplaceTempView("tmp37_spark")
    spark.catalog.uncacheTable("tmp36_1_spark")
    df_tmp36_1.drop()
    tmp38_sql = """
    select 
    t13.store_code,
    '{0}' sale_date,
    t14.date_case date_case
    from tmp1_spark t13
    left join tmp_store_category t14 on t13.store_code=t14.store_code and t14.sale_date='{0}'
    where t14.date_case is not null and t13.store_code is not null
    """.format(item2)
    df_tmp38 = spark.sql(tmp38_sql)
    df_tmp38.createOrReplaceTempView("tmp38_spark")
    spark.catalog.uncacheTable("tmp_store_category")
    df_fct_store_product_on_sale.drop()

    tmp39_sql = """
    select 
    t8.store_code,
    t8.sale_date,
    t8.date_case,
    t12.turnover,
    t9.pre_turnover pre,
    t9.temp_turnover temp,
    t10.recent_3,
    t11.recent_7
    from tmp38_spark t8
    left join tmp32_spark t9 on t8.store_code=t9.store_code 
    left join tmp36_spark t10 on t8.store_code=t10.store_code and t8.date_case=t10.date_case 
    left join tmp37_spark t11 on t8.store_code=t11.store_code and t8.date_case=t11.date_case 
    left join tmp_mid_turnover_feat_sql_spark t12 on t8.store_code=t12.store_code and t8.sale_date=t12.sale_date
    where t8.sale_date='{0}' and t10.recent_3 is not null and t10.recent_3>=0 and t11.recent_7 is not null and t11.recent_7>=0
    """.format(item2)
    df_tmp39 = spark.sql(tmp39_sql)
    df_tmp39 = df_tmp39.dropDuplicates()
    df_tmp39.createOrReplaceTempView("tmp39")
    spark.catalog.uncacheTable("tmp_mid_turnover_feat_sql_spark")
    print("开始插入数据")
    # spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
    # spark.sql("set hive.exec.reducers.bytes.per.reducer=1024000000") partition(month)
    insert_sql = """
    insert into table rbu_sxcp_edw_ai_dev.turnover_feat 
    (select 
    store_code,
    sale_date,
    date_case,
    turnover,
    pre,
    temp,
    recent_3,
    recent_7,
    current_timestamp()
    from tmp39)
    """
    print("插入数据")
    start = time.time()
    spark.sql(insert_sql)
    end = time.time()
    print('Running time: %s Seconds' % (end - start))
    print("插入数据成功")


for i in range(0, 1):
    print("营业额特征数据的代码")
    min_day = datetime.datetime.strptime('2019-01-03', "%Y-%m-%d") + datetime.timedelta(days=i)
    one_year_age = datetime.datetime.strptime(str(min_day)[:10], "%Y-%m-%d") + datetime.timedelta(days=-365)
    print(str(min_day)[:10])
    jiaoben(str(one_year_age)[:10], str(min_day)[:10])


spark.catalog.uncacheTable("tmp_fct_store_product_on_sale")
spark.catalog.uncacheTable("tmp_rst_weather")
spark.catalog.uncacheTable("tmp_dim_store")
spark.catalog.uncacheTable("tmp_store_category")
spark.catalog.uncacheTable("tmp_fct_store_sale_add")
spark.catalog.uncacheTable("tmp_dim_store_product")
spark.catalog.uncacheTable("tmp_fct_pos_detail")
spark.catalog.uncacheTable("tmp3_spark")
print("清空缓存表成功")
print("process successfully!")
print("=====================")
print("到此结束")
