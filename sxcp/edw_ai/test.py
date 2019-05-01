

'''
import os
import threading

mon = ['/home/sxcp/edw/check_daily_stock/first', '/home/sxcp/edw/check_daily_stock/mon_9',
       '/home/sxcp/edw/check_daily_stock/mon_10', '/home/sxcp/edw/check_daily_stock/mon_11',
       '/home/sxcp/edw/check_daily_stock/mon_12', '/home/sxcp/edw/check_daily_stock/mon_1']
scipt = ['/home/sxcp/rst/mon_9', '/home/sxcp/rst/mon_10', '/home/sxcp/rst/mon_11',
         '/home/sxcp/rst/mon_12', '/home/sxcp/rst/mon_1']
item_predict_type = ['/home/sxcp/edw_ai/product_predict_type/product_predict_type.py']
item_digital_warehouse = ['/home/sxcp/edw_ai/digital_warehouse/test']


def jiaoben():
    print("开始库存盘点")
    for item in mon:
        os.system('spark2-submit --num-executors=4 --executor-memory=5G --executor-cores=4 --driver-memory 8g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 %s.py' % item)
    print("开始跑七天销量排行")
    for item in scipt:
        os.system('spark2-submit --num-executors=6 --executor-memory=5G --executor-cores=6 --driver-memory 8g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 %s.py' % item)

    print("商品预测类型")
    for i in item_predict_type:
        os.system('spark2-submit --num-executors=4 --executor-memory=5G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 %s' % i)

    print("库龄")
    for i in item_digital_warehouse:
        print(i)
        os.system('spark2-submit --num-executors=4 --executor-memory=5G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.storage.memoryFraction=0.9 %s.py' % i)


threading.Timer(14400, jiaoben).start()

'''