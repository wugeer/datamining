+ ods_oitg

sqoop --options-file sqoop_option.txt  --table ods_oitg --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_oitg --check-column etl_time  --incremental append --last-value '2018-08-24 03:49:37 '   --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_oitt

sqoop --options-file sqoop_option.txt  --table ods_oitt --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_oitt --check-column etl_time  --incremental append --last-value '2018-08-24 03:47:50'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_osal
 
sqoop --options-file sqoop_option.txt  --table ods_osal --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_osal --check-column etl_time  --incremental append --last-value '2018-12-14 03:47:51'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_ostr

sqoop --options-file sqoop_option.txt  --table ods_ostr --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_ostr --check-column docdate  --incremental append --last-value '2018-12-12 23:33:51'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_owor

sqoop --options-file sqoop_option.txt  --table ods_owor --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_owor --check-column docdate  --incremental append --last-value '2018-12-12 18:52:20'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_owsl

sqoop --options-file sqoop_option.txt  --table ods_owsl --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_owsl --check-column etl_time  --incremental append --last-value '2018-12-13 03:51:19'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_owsr

sqoop --options-file sqoop_option.txt  --table ods_owsr --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_owsr --check-column etl_time  --incremental append --last-value '2018-12-13 03:51:55'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_product_bill_other

sqoop --options-file sqoop_option.txt  --table ods_product_bill_other --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_product_bill_other --check-column bill_date  --incremental append --last-value '2018-12-24'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

+ ods_product_bill_shop

sqoop --options-file sqoop_option.txt  --table ods_product_bill_shop --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_product_bill_shop --check-column bill_date  --incremental append --last-value '2018-12-24'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

+ ods_sal1

sqoop --options-file sqoop_option.txt  --table ods_sal1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_sal1 --check-column etl_time  --incremental append --last-value '2018-12-14 03:49:43'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_sal2

sqoop --options-file sqoop_option.txt  --table ods_sal2 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_sal2 --check-column etl_time  --incremental append --last-value '2018-12-13 03:51:14'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_sck1

sqoop --options-file sqoop_option.txt  --table ods_sck1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_sck1 --check-column etl_time  --incremental append --last-value '2018-12-14 03:51:36'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_str1

sqoop --options-file sqoop_option.txt  --table ods_str1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_str1 --check-column exitdate  --incremental append --last-value '2018-12-13 00:01:00'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_target_product_turnover

sqoop --options-file sqoop_option.txt  --table ods_target_product_turnover --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_target_product_turnover --check-column sale_date  --incremental append --last-value '2018-09-24'    --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

+ ods_wor1

sqoop --options-file sqoop_option.txt  --table ods_wor1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_wor1 --check-column duedate  --incremental append --last-value '2018-12-12 18:52:26'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_wsl1

sqoop --options-file sqoop_option.txt  --table ods_wsl1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_wsl1 --check-column etl_time  --incremental append --last-value '2018-12-14 03:50:34'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

+ ods_wsr1

sqoop --options-file sqoop_option.txt  --table ods_wsr1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_wsr1 --check-column etl_time  --incremental append --last-value  '2018-12-13 03:51:59'  --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

