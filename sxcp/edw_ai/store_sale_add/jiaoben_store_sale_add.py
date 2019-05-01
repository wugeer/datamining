import os

item = [9, 10, 11, 12, 1]

for i in item:
    os.system('spark2-submit --num-executors=4 --executor-memory=6G --executor-cores=4 --driver-memory 8g --conf spark.default.parallelism=200 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 store_sale_add_%s.py'%i)


