import os
mon = ['mon_9','mon_10', 'mon_11', 'mon_12', 'mon_1']

for item in mon:
    os.system('spark2-submit --num-executors=4 --executor-memory=5G --executor-cores=4 --driver-memory 8g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 %s.py'%item)