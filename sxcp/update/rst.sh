#! /bin/sh
export JAVA_HOME=/opt/java/jdk1.8.0_131
export PATH=$PATH:$JAVA_HOME/bin

basedir=/home/sxcp/update

echo 开始增量插入seven_days_turnover_ranking
echo seven_days_turnover_ranking_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/rst_log/seven_days_turnover_ranking.log
spark2-submit --num-executors=2 --executor-memory=2G --executor-cores=2 --driver-memory 4g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/seven_days_turnover_ranking.py >> /home/sxcp/update/rst_log/seven_days_turnover_ranking.log 2>&1
echo seven_days_turnover_ranking_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/rst_log/seven_days_turnover_ranking.log


echo 开始更新rst_dim_product
echo rst_dim_product_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/rst_log/rst_dim_product.log
spark2-submit --num-executors=2 --executor-memory=2G --executor-cores=2 --driver-memory 4g --conf spark.default.parallelism=50 --conf spark.yarn.executor.memoryOverhead=1024 $basedir/rst_dim_product.py >> /home/sxcp/update/rst_log/seven_days_turnover_ranking.log 2>&1
echo rst_dim_product_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/rst_log/rst_dim_product.log