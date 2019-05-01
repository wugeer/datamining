#! /bin/sh
export JAVA_HOME=/opt/java/jdk1.8.0_131
export PATH=$PATH:$JAVA_HOME/bin

basedir=/home/sxcp/update

echo 开始更新dim_store
echo dim_store_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/dim_store.log
spark2-submit --num-executors=2 --executor-memory=4G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/dim_store.py >> /home/sxcp/update/edw_log/dim_store.log 2>&1
echo dim_store_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/dim_store.log

echo 开始更新dim_product
echo dim_product_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/dim_product.log
spark2-submit --num-executors=2 --executor-memory=4G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/dim_product.py >> /home/sxcp/update/edw_log/dim_product.log 2>&1
echo dim_product_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/dim_product.log

echo 开始更新dim_store_product
echo dim_store_product_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/dim_store_product.log
spark2-submit --num-executors=2 --executor-memory=2G --executor-cores=2 --driver-memory 4g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/dim_store_product.py >> /home/sxcp/update/edw_log/dim_store_product.log 2>&1
echo dim_store_product_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/dim_store_product.log

echo 开始增量插入fct_daily_stock
echo fct_daily_stock_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/fct_daily_stock.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/fct_daily_stock.py  >> /home/sxcp/update/edw_log/fct_daily_stock.log 2>&1
echo fct_daily_stock_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/fct_daily_stock.log

echo 开始增量插入fct_io
echo fct_io_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/fct_io.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/fct_io.py >> /home/sxcp/update/edw_log/fct_io.log 2>&1
echo fct_io_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/fct_io.log

echo 开始增量插入fct_pos
echo fct_pos_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/fct_pos.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/fct_pos_detail.py >> /home/sxcp/update/edw_log/fct_pos.log 2>&1
echo fct_pos_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/fct_pos.log

echo 开始增量插入fct_on_sale
echo fct_on_sale_start_time: `date "+%Y-%m-%d %H:%M:%S"`  >> /home/sxcp/update/edw_log/fct_on_sale.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/fct_on_sale.py  >> /home/sxcp/update/edw_log/fct_on_sale.log 2>&1
echo ffct_on_sale_end_time: `date "+%Y-%m-%d %H:%M:%S"`  >> /home/sxcp/update/edw_log/fct_on_sale.log

echo 开始增量插入fct_on_sale_history
echo fct_on_sale_history_start_time: `date "+%Y-%m-%d %H:%M:%S"`  >> /home/sxcp/update/edw_log/fct_store_product_on_sale.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/fct_on_sale_history.py  >> /home/sxcp/update/edw_log/fct_store_product_on_sale.log 2>&1
echo ffct_on_sale_history_end_time: `date "+%Y-%m-%d %H:%M:%S"`  >> /home/sxcp/update/edw_log/fct_store_product_on_sale.log

echo 开始增量插入check_daily_stock
echo check_daily_stock_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/check_daily_stock.log
spark2-submit --num-executors=2 --executor-memory=6G --executor-cores=2 --driver-memory 8g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/check_daily_stock.py >> /home/sxcp/update/edw_log/check_daily_stock.log 2>&1
echo check_daily_stock_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/edw_log/check_daily_stock.log