# gp_to_hive

-- ods_display
sqoop --options-file sqoop_option.txt --table ods_display --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_display  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1

-- ods_ocus
sqoop --options-file sqoop_option.txt --table ods_ocus --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_ocus  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1

-- ods_ohem
sqoop --options-file sqoop_option.txt --table ods_ohem --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_ohem  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1

-- ods_oitm
sqoop --options-file sqoop_option.txt --table ods_oitm --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_oitm  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1

-- ods_oshp
sqoop --options-file sqoop_option.txt --table ods_oshp --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_oshp  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1


-- ods_ousr
sqoop --options-file sqoop_option.txt --table ods_ousr --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_ousr  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1

-- ods_sa_personuprice_501
sqoop --options-file sqoop_option.txt --table ods_sa_personuprice_501 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_sa_personuprice_501  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1

-- ods_sa_personuprice_801
sqoop --options-file sqoop_option.txt --table ods_sa_personuprice_801 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_sa_personuprice_801  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1

-- ods_shelf_life
sqoop --options-file sqoop_option.txt --table ods_shelf_life --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_shelf_life  --hive-overwrite --null-string '\\\\N'  --null-non-string '\\\\N' --hive-drop-import-delims -m 1 -- --schema ods1


