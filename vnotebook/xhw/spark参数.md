# spark参数
```
spark2-submit --driver-memory 16g --conf spark.storage.memoryFraction=0.9 xx.py
最初是5700秒
spark2-submit --num-executors=8 --executor-memory=1G --executor-cores=2 --driver-memory 4g --conf spark.default.parallelism=500 --conf spark.yarn.executor.memoryOverhead=2048 --conf spark.storage.memoryFraction=0.9 mon_6.py 
2076秒
spark2-submit --num-executors=8 --executor-memory=2G --executor-cores=4 --driver-memory 4g --conf spark.default.parallelism=500 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 mon_6.py 
2100秒

spark2-submit --num-executors=8 --executor-memory=2G --executor-cores=4 --driver-memory 4g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 mon_6.py 
1980秒


spark2-submit --num-executors=8 --executor-memory=2G --executor-cores=4 --driver-memory 6g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 sale_add_sum.py 
```


```
CREATE TABLE `rbu_sxcp_edw_dev.mid_sale_add_avg`(
  `store_code` string, 
  `product_code` string, 
  `sale_date` string, 
  `date_case` string, 
  `avg_3_sale` decimal(30,8), 
  `avg_4_sale` decimal(30,8), 
  `avg_5_sale` decimal(30,8), 
  `avg_6_sale` decimal(30,8), 
  `avg_7_sale` decimal(30,8), 
  `avg_8_sale` decimal(30,8), 
  `avg_9_sale` decimal(30,8), 
  `avg_10_sale` decimal(30,8), 
  `avg_11_sale` decimal(30,8), 
  `avg_12_sale` decimal(30,8), 
  `avg_13_sale` decimal(30,8), 
  `avg_14_sale` decimal(30,8), 
  `avg_15_sale` decimal(30,8), 
  `avg_16_sale` decimal(30,8), 
  `avg_17_sale` decimal(30,8), 
  `avg_18_sale` decimal(30,8), 
  `avg_19_sale` decimal(30,8), 
  `avg_20_sale` decimal(30,8), 
  `avg_21_sale` decimal(30,8), 
  `avg_22_sale` decimal(30,8), 
  `avg_23_sale` decimal(30,8), 
  `avg_0_sale` decimal(30,8), 
  `avg_1_sale` decimal(30,8), 
  `avg_2_sale` decimal(30,8), 
  `etl_time` string)

```