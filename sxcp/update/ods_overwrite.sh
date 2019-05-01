#! /bin/sh
export JAVA_HOME=/opt/java/jdk1.8.0_131
export PATH=$PATH:$JAVA_HOME/bin

echo 开始全量导入ods_display
echo ods_display_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_display.log
sqoop --options-file sqoop_option.txt --table ods_display --hive-database rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_display  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_display.log 2>&1
echo ods_display_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_display.log

echo 开始全量导入ods_ocus
echo ods_ocus_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_ocus.log
sqoop --options-file sqoop_option.txt --table ods_ocus --hive-database rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_ocus  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_ocus.log 2>&1
echo ods_ocus_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_ocus.log

echo 开始全量导入ods_ohem
echo ods_ohem_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_ohem.log
sqoop --options-file sqoop_option.txt --table ods_ohem --hive-database rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_ohem  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_ohem.log 2>&1
echo ods_ohem_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_ohem.log

echo 开始全量导入ods_oitm
echo ods_oitm_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_oitm.log
sqoop --options-file sqoop_option.txt --table ods_oitm --hive-database rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_oitm  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_oitm.log 2>&1
echo ods_oitm_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_oitm.log

echo 开始全量导入ods_oshp
echo ods_oshp_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_oshp.log
sqoop --options-file sqoop_option.txt --table ods_oshp --hive-database rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_oshp  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_oshp.log 2>&1
echo ods_oshp_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_oshp.log

echo 开始全量导入ods_ousr
echo ods_ousr_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_ousr.log
sqoop --options-file sqoop_option.txt --table ods_ousr --hive-database rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_ousr  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_ousr.log 2>&1
echo ods_ousr_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_ousr.log

echo 开始全量导入ods_sa_personuprice_501
echo ods_sa_personuprice_501_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_sa_personuprice_501.log
sqoop --options-file sqoop_option.txt --table ods_sa_personuprice_501 --hive-database  rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_sa_personuprice_501  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_sa_personuprice_501.log 2>&1
echo ods_sa_personuprice_501_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_sa_personuprice_501.log

echo 开始全量导入ods_sa_personuprice_801
echo ods_sa_personuprice_801_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_sa_personuprice_801.log
sqoop --options-file sqoop_option.txt --table ods_sa_personuprice_801 --hive-database  rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_sa_personuprice_801  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_sa_personuprice_801.log 2>&1
echo ods_sa_personuprice_801_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_sa_personuprice_801.log

echo 开始全量导入ods_shelf_life
echo ods_shelf_life_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_shelf_life.log
sqoop --options-file sqoop_option.txt --table ods_shelf_life --hive-database  rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_shelf_life  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_shelf_life.log 2>&1
echo ods_shelf_life_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_shelf_life.log

echo 开始全量导入ods_customer_501
echo ods_customer_501_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_customer_501.log
sqoop --options-file sqoop_option.txt --table ods_customer_501 --hive-database  rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_customer_501  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_customer_501.log 2>&1
echo ods_customer_501_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_customer_501.log

echo 开始全量导入ods_customer_801
echo ods_customer_801_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_customer_801.log
sqoop --options-file sqoop_option.txt --table ods_customer_801 --hive-database  rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_customer_801  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/ods_customer_801.log 2>&1
echo ods_customer_801_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_customer_801.log

echo 开始全量导入ods_itm1
echo ods_itm1_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_itm1.log
sqoop --options-file sqoop_option.txt --table ods_itm1 --hive-database  rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table ods_itm1  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods >> /home/sxcp/update/ods_log/ods_itm1.log 2>&1
echo ods_itm1_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/ods_itm1.log

echo 开始全量导入rst_weather
echo rst_weather_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/rst_weather.log
sqoop --options-file sqoop_option.txt --table rst_weather --hive-database rbu_sxcp_ods_dev --delete-target-dir --hive-import --hive-table rst_weather --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema rst >> /home/sxcp/update/ods_log/rst_weather.log 2>&1
echo rst_weather_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/rst_weather.log

#echo 开始全量导入date_case
#echo date_case_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/date_case.log
#sqoop --options-file sqoop_option.txt --table rst_shop_date_case --hive-database  rbu_sxcp_edw_ai_dev  --delete-target-dir --hive-import --hive-table store_category  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/date_case.log 2>&1
#echo date_case_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/date_case.log

echo 开始全量导入rst_store_product
echo rst_store_product_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/rst_store_product.log
sqoop import --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail --username postgres --password lzsj1701 --table rst_store_product --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table rst_store_product  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema rst >> /home/sxcp/update/ods_log/rst_store_product.log 2>&1
echo rst_store_product_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/rst_store_product.log

echo 开始全量导入rst_product_arrive
echo rst_product_arrive_start_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/rst_product_arrive.log
sqoop --options-file sqoop_option.txt --table rst_product_arrive --hive-database  rbu_sxcp_tmp  --delete-target-dir --hive-import --hive-table rst_product_arrive  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 >> /home/sxcp/update/ods_log/rst_product_arrive.log 2>&1
echo rst_product_arrive_end_time: `date "+%Y-%m-%d %H:%M:%S"` >> /home/sxcp/update/ods_log/rst_product_arrive.log


