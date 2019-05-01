from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
import datetime
import time

mon = ['2019-01-01', '2019-01-08']
spark = SparkSession.builder.master("yarn").appName("fct_daily_stock").getOrCreate()
print("starting.......")
tmp1_sql = """
select 
c.Shpcode store_code,
d.itemcode product_code,
b.Qty1 qty,
to_date(b.enterdate) loss_date
from rbu_sxcp_ods_dev.ods_owsr a
left join rbu_sxcp_ods_dev.ods_wsr1 b on a.docentry=b.docentry and a.enterdate=b.enterdate and a.shpentry=b.shpentry
left join rbu_sxcp_ods_dev.ods_oshp c on b.shpentry=c.docentry
left join rbu_sxcp_ods_dev.ods_oitm d on b.itementry=d.docentry 
where a.reason='产品报废' and c.shpcode is not null and to_date(b.enterdate)>='{0}' and to_date(b.enterdate)<'{1}' and d.itemcode is not null
""".format(mon[0], mon[1])
df_tmp1 = spark.sql(tmp1_sql)
df_tmp1.createOrReplaceTempView("tmp1")
# print("tmp1")
# df_tmp1.show()

tmp2_sql = """
select 
store_code,
product_code,
sum(qty) qty,
loss_date
from tmp1
group by store_code,product_code,loss_date
"""
df_tmp2 = spark.sql(tmp2_sql)
df_tmp2.createOrReplaceTempView("loss")
#spark.catalog.cacheTable("loss")
# print("loss")
# df_tmp2.show()

tmp3_sql = """
SELECT store_code, product_code, qty,io_date
FROM rbu_sxcp_edw_dev.fct_io
where io_type in ('调拨出库', '出库', '订单') and store_code is not null and product_code is not null
"""
df_tmp3 = spark.sql(tmp3_sql)
df_tmp3.createOrReplaceTempView("tmp3")

tmp4_sql = """
select store_code,product_code,io_date,sum(qty) out_qty
from tmp3
group by store_code,product_code,io_date
"""
df_tmp4 = spark.sql(tmp4_sql)
df_tmp4.createOrReplaceTempView("out_io")
#spark.catalog.cacheTable("out_io")
# print("out_io")
# df_tmp4.show()

tmp5_sql = """
SELECT store_code, product_code, qty,io_date
FROM rbu_sxcp_edw_dev.fct_io
where io_type in ('调拨入库', '入库') and store_code is not null and product_code is not null
"""#
df_tmp5 = spark.sql(tmp5_sql)
df_tmp5.createOrReplaceTempView("tmp5")

tmp6_sql = """
select store_code,product_code,io_date,sum(qty) in_qty
from tmp5
group by store_code,product_code,io_date
"""
df_tmp6 = spark.sql(tmp6_sql)
df_tmp6.createOrReplaceTempView("in_io")
#spark.catalog.cacheTable("in_io")
# print("in_io")
# df_tmp6.show()

fct_daily_stock_sql = """
SELECT store_code, product_code, qty, check_date
FROM rbu_sxcp_edw_dev.fct_daily_stock
where store_code is not null and product_code is not null
"""
df_fct_daily_stock = spark.sql(fct_daily_stock_sql)
df_fct_daily_stock.createOrReplaceTempView("fct_daily_stock")
# print("fct_daily_stock")
# df_fct_daily_stock.show()

tmp7_sql = """
select store_code,product_code,to_date(check_date) check_date,sum(qty) daily_stock_qty
from fct_daily_stock
group by store_code,product_code,check_date
"""
df_tmp7 = spark.sql(tmp7_sql)
df_tmp7.createOrReplaceTempView("tmp7")
#spark.catalog.cacheTable("tmp7")
# print("tmp7")
# df_tmp7.show()
store_product_sql = """
select store_code,product_code from rbu_sxcp_edw_dev.dim_store_product
"""
df_store_product = spark.sql(store_product_sql)
df_store_product = df_store_product.dropDuplicates()
df_store_product.createOrReplaceTempView("store_product")
#spark.catalog.cacheTable("store_product")


def jiaoben(item):
    # 昨日盘点数量
    yesterday_stock = """
    select a.store_code,a.product_code,'{0}' check_date,b.daily_stock_qty
    from store_product a
    left join tmp7 b on a.store_code=b.store_code and a.product_code=b.product_code and b.check_date='{0}'
    """.format(item)
    df_yesterday_stock = spark.sql(yesterday_stock)
    df_yesterday_stock.createOrReplaceTempView("yesterday_stock")
    # print("yesterday_stock")
    # df_yesterday_stock.show()
    # 前天盘点数量
    before_day_stock = """
    select a.store_code,a.product_code,date_sub('{0}',1) check_date,b.daily_stock_qty
    from store_product a
    left join tmp7 b on a.store_code=b.store_code and a.product_code=b.product_code and b.check_date=date_sub('{0}',1)
    """.format(item)
    df_before_day_stock = spark.sql(before_day_stock)
    df_before_day_stock.createOrReplaceTempView("before_day_stock")
    # print("before_day_stock")
    # df_before_day_stock.show()

    # 取出前天计算库存
    check_daily_stock = """
    SELECT store_code, product_code, check_date, calculate_end_qty
    FROM rbu_sxcp_edw_dev.check_daily_stock
    where check_date=date_sub('{0}',1)
    """.format(item)
    df_check_daily_stock = spark.sql(check_daily_stock)
    df_check_daily_stock.createOrReplaceTempView("check_daily_stock")
    # print("check_daily_stock")
    # df_check_daily_stock.show()

    tmp8_sql = """
    select a.store_code,
    a.product_code,
    a.check_date,
    coalesce(e.daily_stock_qty, f.calculate_end_qty, 0)+coalesce(d.in_qty, 0)-coalesce(b.qty, 0)-coalesce(c.out_qty, 0) calculate_end_qty
    from yesterday_stock a 
    left join loss b on a.store_code=b.store_code and a.product_code=b.product_code and a.check_date=b.loss_date
    left join out_io c on a.store_code=c.store_code and a.product_code=c.product_code and a.check_date=c.io_date
    left join in_io d on a.store_code=d.store_code and a.product_code=d.product_code and a.check_date=d.io_date
    left join before_day_stock e on a.store_code=e.store_code and a.product_code=e.product_code and a.check_date=date_add(e.check_date,1)
    left join check_daily_stock f on a.store_code=f.store_code and a.product_code=f.product_code and a.check_date=date_add(f.check_date,1)
    """
    df_tmp8 = spark.sql(tmp8_sql)
    df_tmp8.createOrReplaceTempView("tmp8")
    #print("tmp8")
    #df_tmp8.show()

    final_sql = """
    select
    a.store_code,
    a.product_code,
    a.check_date,
    b.calculate_end_qty,
    a.daily_stock_qty,
    case when a.daily_stock_qty is null then 0
         when b.calculate_end_qty=a.daily_stock_qty then 1
         else 2
    end check_status
    from yesterday_stock a 
    left join tmp8 b on a.store_code=b.store_code and a.product_code=b.product_code and a.check_date=b.check_date
    """
    df_final = spark.sql(final_sql)
    df_final = df_final.dropDuplicates()
    df_final.createOrReplaceTempView("final")
    #print("final")
    #df_final.show()

    insert_sql = """
    insert into table rbu_sxcp_edw_dev.check_daily_stock
    (select store_code,
    product_code,
    check_date,
    calculate_end_qty,
    daily_stock_qty,
    check_status,
    current_timestamp()
    from final)
    """
    print("开始插入数据")
    start = time.time()
    spark.sql(insert_sql)
    end = time.time()
    print('Running time: %s Seconds' % (end - start))
    print("插入数据成功")


for i in range(0, 7):
    print("check_daily_stock")
    min_day = datetime.datetime.strptime('2019-01-01', "%Y-%m-%d") + datetime.timedelta(days=i)
    print(str(min_day)[:10])
    jiaoben(str(min_day)[:10])

spark.catalog.uncacheTable("loss")
spark.catalog.uncacheTable("out_io")
spark.catalog.uncacheTable("in_io")
spark.catalog.uncacheTable("tmp7")
print("process successfully!")
print("=====================")