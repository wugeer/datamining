import os
import datetime

from datetime import datetime, date, timedelta
item_yesterday = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
item_today = date.today().strftime("%Y-%m-%d")

update_ods = ["sqoop --options-file sqoop_option.txt  --table ods_osal --hive-database  rbu_sxcp_ods_dev --hive-import --hive-table ods_osal --check-column etl_time  --incremental append --last-value '%s 00:00:00' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods"%item_today,
          "sqoop --options-file sqoop_option.txt  --table ods_ostr --hive-database  rbu_sxcp_ods_dev --hive-import --hive-table ods_ostr --check-column docdate  --incremental append --last-value '%s 00:00:00' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods"%item_yesterday,
          "sqoop --options-file sqoop_option.txt  --table ods_owor --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_owor --check-column docdate  --incremental append --last-value '%s 00:00:00' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods"%item_yesterday,
          "sqoop --options-file sqoop_option.txt  --table ods_owsl --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_owsl --check-column etl_time  --incremental append --last-value '%s 00:00:00' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods"%item_today,
          "sqoop --options-file sqoop_option.txt  --table ods_owsr --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_owsr --check-column etl_time  --incremental append --last-value '%s 00:00:00' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods"%item_today,
          "sqoop --options-file sqoop_option.txt  --table ods_sal1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_sal1 --check-column etl_time  --incremental append --last-value '2019-01-03 03:49:43' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods",
          "sqoop --options-file sqoop_option.txt  --table ods_sal2 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_sal2 --check-column etl_time  --incremental append --last-value '2019-01-03 03:51:14' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods",
          "sqoop --options-file sqoop_option.txt  --table ods_sck1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_sck1 --check-column etl_time  --incremental append --last-value '2019-01-03 03:54:36' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods",
          "sqoop --options-file sqoop_option.txt  --table ods_str1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_str1 --check-column exitdate  --incremental append --last-value '2019-01-02 00:00:01' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods",
          "sqoop --options-file sqoop_option.txt  --table ods_wor1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_wor1 --check-column duedate  --incremental append --last-value '2019-01-02 18:29:26' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods",
          "sqoop --options-file sqoop_option.txt  --table ods_wsl1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_wsl1 --check-column etl_time  --incremental append --last-value '2019-01-03 03:50:55' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods",
          "sqoop --options-file sqoop_option.txt  --table ods_wsr1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_wsr1 --check-column etl_time  --incremental append --last-value  '2019-01-03 03:51:01' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods",
          "sqoop --options-file sqoop_option.txt  --table ods_itm1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_itm1 --check-column etl_time  --incremental append --last-value '2019-01-03 03:46:58' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods"    ]

update_ods1 = ["sqoop --options-file sqoop_option.txt --table ods_target_product_turnover --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_target_product_turnover --check-column sale_date  --incremental append --last-value '2018-09-24' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1",
               "sqoop --options-file sqoop_option.txt  --table ods_product_bill_shop --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_product_bill_shop --check-column bill_date  --incremental append --last-value '2018-12-24' --null-string '\\\\N' --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1",
               "sqoop --options-file sqoop_option.txt  --table ods_product_bill_other --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_product_bill_other --check-column bill_date  --incremental append --last-value '2018-12-24' --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1"]

print("增量导入ods数据")
for item in update_ods:
    os.system(item)

print("增量导入ods1数据")
for item in update_ods1:
    os.system(item)
