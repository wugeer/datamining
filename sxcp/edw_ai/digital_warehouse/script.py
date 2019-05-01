import os

item = ['/home/sxcp/edw_ai/digital_warehouse/test']

for i in item:
    print(i)
    os.system('spark2-submit --num-executors=4 --executor-memory=5G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 %s.py'%i)

