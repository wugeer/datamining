import os

history = [9, 10, 11, 12, 1]

print("开始更新事实表数据")
for item in history:
    os.system('spark2-submit --num-executors=4 --executor-memory=3G --executor-cores=4 --driver-memory 5g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 history_%s.py'%item)
