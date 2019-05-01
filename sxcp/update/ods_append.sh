#! /bin/sh
export JAVA_HOME=/opt/java/jdk1.8.0_131
export PATH=$PATH:$JAVA_HOME/bin

yestoday=`date -d "-1 day" +%Y-%m-%d`
today=`date "+%Y-%m-%d"`
twoday=`date -d "-2 day" +%Y-%m-%d`
sqoop_option=/home/sxcp/update/sqoop_option.txt

echo 开始增量导入ods_osal
echo ods_osal_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_osal.log
sqoop --options-file $sqoop_option --table ods_osal --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_osal --check-column etl_time  --incremental append --last-value \""$today 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_osal.log 2>&1
echo ods_osal_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_osal.log

echo 开始增量导入ods_owsl
echo ods_owsl_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_owsl.log
sqoop --options-file $sqoop_option --table ods_owsl --hive-database rbu_sxcp_ods_dev  --hive-import --hive-table ods_owsl --check-column etl_time  --incremental append --last-value \""$today 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_owsl.log 2>&1
echo ods_owsl_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_owsl.log

echo 开始增量导入ods_owsr
echo ods_owsr_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_owsr.log
sqoop --options-file $sqoop_option --table ods_owsr --hive-database rbu_sxcp_ods_dev  --hive-import --hive-table ods_owsr --check-column etl_time  --incremental append --last-value \""$today 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_owsr.log 2>&1
echo ods_owsr_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_owsr.log

echo 开始增量导入ods_sal1
echo ods_sal1_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_sal1.log
sqoop --options-file $sqoop_option --table ods_sal1 --hive-database rbu_sxcp_ods_dev  --hive-import --hive-table ods_sal1 --check-column etl_time  --incremental append --last-value \""$today 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_sal1.log 2>&1
echo ods_sal1_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_sal1.log

echo 开始增量导入ods_sal2
echo ods_sal2_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_sal2.log
sqoop --options-file $sqoop_option --table ods_sal2 --hive-database  rbu_sxcp_ods_dev --hive-import --hive-table ods_sal2 --check-column etl_time  --incremental append --last-value \""$today 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_sal2.log 2>&1
echo ods_sal2_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_sal2.log

echo 开始增量导入ods_sck1
echo ods_sck1_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_sck1.log
sqoop --options-file $sqoop_option --table ods_sck1 --hive-database  rbu_sxcp_ods_dev --hive-import --hive-table ods_sck1 --check-column etl_time  --incremental append --last-value \""$today 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_sck1.log 2>&1
echo ods_sck1_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_sck1.log

echo 开始增量导入ods_wsl1
echo ods_wsl1_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_wsl1.log
sqoop --options-file $sqoop_option --table ods_wsl1 --hive-database  rbu_sxcp_ods_dev --hive-import --hive-table ods_wsl1 --check-column etl_time  --incremental append --last-value \""$today 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_wsl1.log 2>&1
echo ods_wsl1_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_wsl1.log

echo 开始增量导入ods_wsr1
echo ods_wsr1_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_wsr1.log
sqoop --options-file $sqoop_option --table ods_wsr1 --hive-database  rbu_sxcp_ods_dev --hive-import --hive-table ods_wsr1 --check-column etl_time  --incremental append --last-value \""$today 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_wsr1.log 2>&1
echo ods_wsr1_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_wsr1.log

echo 开始增量导入ods_owor
echo ods_owor_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_owor.log
sqoop --options-file $sqoop_option --table ods_owor --hive-database  rbu_sxcp_ods_dev --hive-import --hive-table ods_owor --check-column docdate  --incremental append --last-value \""$yestoday 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_owor.log 2>&1
echo ods_owor_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_owor.log

echo 开始增量导入ods_ostr
echo ods_ostr_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_ostr.log
sqoop --options-file $sqoop_option --table ods_ostr --hive-database  rbu_sxcp_ods_dev --hive-import --hive-table ods_ostr --check-column docdate  --incremental append --last-value \""$yestoday 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_ostr.log 2>&1
echo ods_ostr_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_ostr.log

echo 开始增量导入ods_product_bill_shop
echo ods_product_bill_shop_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_product_bill_shop.log
sqoop --options-file $sqoop_option --table ods_product_bill_shop --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_product_bill_shop --check-column bill_date  --incremental append --last-value \""$yestoday"\" --null-string '\\N' --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_product_bill_shop.log 2>&1
echo ods_product_bill_shop_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_product_bill_shop.log

echo 开始增量导入ods_product_bill_other
echo ods_product_bill_other_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_product_bill_other.log
sqoop --options-file $sqoop_option --table ods_product_bill_other --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_product_bill_other --check-column bill_date  --incremental append --last-value \""$yestoday"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_product_bill_other.log 2>&1
echo ods_product_bill_other_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_product_bill_other.log

echo 开始增量导入ods_str1
echo ods_str1_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_str1.log
sqoop --options-file $sqoop_option --table ods_str1 --hive-database  rbu_sxcp_ods_dev --hive-import --hive-table ods_str1 --check-column exitdate  --incremental append --last-value \""$twoday 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_str1.log 2>&1
echo ods_str1_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_str1.log

echo 开始增量导入ods_wor1
echo ods_wor1_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_wor1.log
sqoop --options-file $sqoop_option --table ods_wor1 --hive-database  rbu_sxcp_ods_dev --hive-import --hive-table ods_wor1 --check-column duedate  --incremental append --last-value \""$yestoday 00:00:00"\" --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_wor1.log 2>&1
echo ods_wor1_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_wor1.log
