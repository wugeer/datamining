#! /bin/sh
export JAVA_HOME=/opt/java/jdk1.8.0_131
export PATH=$PATH:$JAVA_HOME/bin

basedir=/home/sxcp/update

echo 开始更新case
echo update_case_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/update_case.log
spark2-submit --num-executors=2 --executor-memory=4G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/update_case.py >> /home/sxcp/update/edw_log/update_case.log 2>&1
echo update_case_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/update_case.log


echo 开始更新sale_add_sum
echo sale_add_sum_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/sale_add_sum.log
spark2-submit --num-executors=2 --executor-memory=4G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/sale_add_sum.py >> /home/sxcp/update/edw_log/sale_add_sum.log 2>&1
echo sale_add_sum_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/sale_add_sum.log

echo 开始更新sale_add_avg
echo sale_add_avg_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/sale_add_avg.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/sale_add_avg.py >> /home/sxcp/update/edw_log/sale_add_avg.log 2>&1
echo sale_add_avg_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/sale_add_avg.log

echo 开始更新store_sale_add
echo store_sale_add_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/store_sale_add.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/store_sale_add.py >> /home/sxcp/update/edw_log/store_sale_add.log 2>&1
echo store_sale_add_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/store_sale_add.log

echo 开始更新turnover_feat
echo turnover_feat_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/turnover_feat.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/turnover_feat.py >> /home/sxcp/update/edw_log/turnover_feat.log 2>&1
echo turnover_feat_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/turnover_feat.log

echo 开始更新sale_feat
echo sale_feat_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/sale_feat.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/sale_feat.py >> /home/sxcp/update/edw_log/sale_feat.log 2>&1
echo sale_feat_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/sale_feat.log

echo 开始更新product_predict_type
echo product_predict_type_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/product_predict_type.log
spark2-submit --executor-memory=4g --driver-memory 8g --conf spark.yarn.executor.memoryOverhead=1024  $basedir/product_predict_type.py >> /home/sxcp/update/edw_log/product_predict_type.log 2>&1
echo product_predict_type_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/product_predict_type.log

echo 开始更新digital_warehouse
echo digital_warehouse_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/digital_warehouse.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/digital_warehouse.py >> /home/sxcp/update/edw_log/digital_warehouse.log 2>&1
echo digital_warehouse_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/digital_warehouse.log

# echo 开始插入sale_predict
# echo sale_predict_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/sale_predict.log
# spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/sale_predict.py >> /home/sxcp/update/edw_log/sale_predict.log 2>&1
# echo sale_predict_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/sale_predict.log

echo 开始更新sale_predict
echo sale_predict_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/sale_predict.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/update_sale_predict.py >> /home/sxcp/update/edw_log/sale_predict.log 2>&1
echo sale_predict_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/sale_predict.log