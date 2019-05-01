import os

dim_script = ["dim_store", "dim_product", "dim_store_product"]

fct_scipt = ["fct_daily_stock", "fct_io", "fct_pos_detail", "fct_store_product_on_sale"]

end = ["fct_store_product_on_sale"]
# print("开始更新维表数据")
# for item in dim_script:
#     os.system('spark2-submit --num-executors=4 --executor-memory=3G --executor-cores=4 --driver-memory 5g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 %s.py'%item)

print("开始更新事实表数据")
for item in end:
    os.system('spark2-submit --num-executors=4 --executor-memory=3G --executor-cores=4 --driver-memory 5g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 %s.py'%item)
