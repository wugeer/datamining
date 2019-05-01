import threading
import os
item = ['sale_feat_9.py', 'sale_feat_10.py', 'sale_feat_11.py', 'sale_feat_12.py', 'sale_feat_1.py']

def jiaoben():
    for i in item:
        os.system('spark2-submit --num-executors=4 --executor-memory=6G --executor-cores=4 --driver-memory 8g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 %s'%i)

threading.Timer(18000, jiaoben).start()

