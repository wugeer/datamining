import os

table_list = ["sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table rst_turnover_predict --hive-database  rbu_sxcp_edw_ai_dev  --delete-target-dir --hive-import --hive-table rst_turnover_predict  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1",
              "sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table rst_sale_predict --hive-database  rbu_sxcp_edw_ai_dev  --delete-target-dir --hive-import --hive-table rst_sale_predict  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1",
              "sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_ocus --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_ocus --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1",
              "sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_oshp --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_oshp --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1 "]

test = ["sqoop --options-file sqoop_option.txt  --table test_append --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table test_append --check-column etl_time  --incremental append --last-value '2018-12-25 18:50:07'   --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema test"]
for item in test:
    os.system(item)




