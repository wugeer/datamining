1. rst_digital_warehouse

sqoop --options-file sqoop_option.txt --table rst_digital_warehouse --hive-database  rbu_sxcp_tmp  --delete-target-dir --hive-import --hive-table rst_digital_warehouse  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

2. rst_product_predict_type

sqoop --options-file sqoop_option.txt --table rst_product_predict_type --hive-database  rbu_sxcp_edw_ai_dev  --delete-target-dir --hive-import --hive-table rst_product_predict_type  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

3. rst_product_arrive

sqoop --options-file sqoop_option.txt --table rst_product_arrive --hive-database  rbu_sxcp_tmp  --delete-target-dir --hive-import --hive-table rst_product_arrive  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

4. rst_product_onsale_history

sqoop --options-file sqoop_option.txt --table rst_product_onsale_history --hive-database  rbu_sxcp_tmp  --delete-target-dir --hive-import --hive-table rst_product_onsale_history  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

5. rst_turnover_predict

sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table rst_turnover_predict --hive-database  rbu_sxcp_tmp  --delete-target-dir --hive-import --hive-table turnover_predict  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

6. rst_weather

sqoop --options-file sqoop_option.txt --table rst_weather --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table rst_weather  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema rst

7. rst_sale_predict

sqoop --options-file sqoop_option.txt --table rst_sale_predict --hive-database  rbu_sxcp_tmp  --delete-target-dir --hive-import --hive-table rst_sale_predict  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1

8. rst_turnover_feat

sqoop --options-file sqoop_option.txt --table rst_turnover_feat --hive-database  rbu_sxcp_tmp  --delete-target-dir --hive-import --hive-table rst_turnover_feat  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1

9. rst_sale_feat

sqoop --options-file sqoop_option.txt --table rst_sale_feat --hive-database  rbu_sxcp_tmp  --delete-target-dir --hive-import --hive-table rst_sale_feat  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1

10 rst_turnover_feat_sum
sqoop --options-file sqoop_option.txt --table rst_turnover_feat_sum --hive-database  rbu_sxcp_tmp  --delete-target-dir --hive-import --hive-table rst_turnover_feat_sum  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1






sqoop  import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table rst_product_arrive --hive-database  rbu_sxcp_edw_ai_dev  --delete-target-dir --hive-import --hive-table rst_product_arrive  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1