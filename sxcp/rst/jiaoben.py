import os
print("开始生成hive的rst层数据")
rst = ["spark2-submit --num-executors=6 --executor-memory=2G --executor-cores=6 --driver-memory 4g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 rst_product_bill.py",
       "spark2-submit --num-executors=6 --executor-memory=2G --executor-cores=6 --driver-memory 4g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 rst_product_qty.py"]


for item in rst:
    os.system(item)

print("开始从hive rst层抽数据到gp")
sqoop_rst = ["sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail --username postgres --password lzsj1701 --table rst_product_bill_1 --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table rst_product_bill --m 1 -- --schema rst",
             "sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail_test --username postgres --password lzsj1701 --table rst_product_bill_1 --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table rst_product_bill --m 1 -- --schema rst",
             "sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail --username postgres --password lzsj1701 --table rst_product_qty_1 --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table rst_product_qty_1 --m 1 -- --schema rst",
             "sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail_test --username postgres --password lzsj1701 --table rst_product_qty_1 --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table rst_product_qty_1 --m 1 -- --schema rst"]


for i in sqoop_rst:
    os.system(i)