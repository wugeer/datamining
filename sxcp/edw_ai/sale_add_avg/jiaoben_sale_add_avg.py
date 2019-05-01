import os

item = ['sale_add_avg_10', 'sale_add_avg_11', 'sale_add_avg_12', 'sale_add_avg_19_1']

for i in item:
    print(i)
    os.system('spark2-submit --num-executors=4 --executor-memory=3G --executor-cores=3 --driver-memory 6g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 %s.py'%i)

