import os

five_rst_table = ["sqoop --options-file sqoop_option.txt --table rst_product_onsale_history --hive-database rbu_sxcp_edw_ai_dev  --delete-target-dir --hive-import --hive-table rst_product_onsale_history  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1",
                  "sqoop  --options-file sqoop_option.txt  --table rst_sale_predict --hive-database  rbu_sxcp_edw_ai_dev  --delete-target-dir --hive-import --hive-table rst_sale_predict  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1",
                  "sqoop  --options-file sqoop_option.txt --table rst_product_arrive --hive-database  rbu_sxcp_edw_ai_dev  --delete-target-dir --hive-import --hive-table rst_product_arrive  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1",
                  "sqoop --options-file sqoop_option.txt --table rst_shop_date_case --hive-database  rbu_sxcp_edw_ai_dev  --delete-target-dir --hive-import --hive-table store_category  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1",
                  "sqoop --options-file sqoop_option.txt --table rst_product_predict_type --hive-database  rbu_sxcp_edw_ai_dev  --delete-target-dir --hive-import --hive-table rst_product_predict_type  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1",
                  "sqoop --options-file sqoop_option.txt --table rst_digital_warehouse --hive-database  rbu_sxcp_edw_ai_dev  --delete-target-dir --hive-import --hive-table rst_digital_warehouse  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1"]
for item in five_rst_table:
    os.system(five_rst_table)
