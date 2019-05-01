import os
print('开始turnover_feat数据生成')
item = ['turnover_feat_9.py', 'turnover_feat_10.py', 'turnover_feat_11.py', 'turnover_feat_12.py', 'turnover_feat_1.py']
for i in item:
    os.system('spark2-submit --num-executors=4 --executor-memory=6G --executor-cores=4 --driver-memory 8g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 %s'%i)


