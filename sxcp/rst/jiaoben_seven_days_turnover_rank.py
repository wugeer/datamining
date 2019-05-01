import os

scipt = ['mon_9', 'mon_10', 'mon_11', 'mon_12', 'mon_1']
print("开始跑七天销量排行")
for item in scipt:
    os.system('spark2-submit --num-executors=6 --executor-memory=3G --executor-cores=6 --driver-memory 6g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 %s.py'%item)
