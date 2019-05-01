from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession

spark = SparkSession.builder.master("yarn").appName("dim_product").enableHiveSupport().getOrCreate()
print("starting..........")
sql_dim_store = """
select * from rbu_sxcp_edw_dev.dim_store
"""
df_store = spark.sql(sql_dim_store)
df_store = df_store.dropDuplicates()
df_store.createOrReplaceTempView("store")
spark.catalog.cacheTable("store")
df_store.show()
insert_sql = """
insert overwrite table rbu_sxcp_edw_dev.dim_store
(select * from store)
"""
spark.sql(insert_sql)

print("process successfully!")
print("=====================")