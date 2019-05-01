# 增量gp_to_hive
+ 事实表:msn_weather,ods_itm1,ods_ocus,ods_ohem,ods_oitg,ods_oitt,ods_osal,ods_ostr,ods_owor,ods_owsr,ods_owsl,ods_product_bill_other,ods_product_bill_shop,ods_sal1,ods_sal2,ods_sck1,ods_str1,ods_target_product_turnover,ods_wsr1,ods_wsl1

+ 全量更新:ods_customer_501,ods_customer_801,ods_display,ods_oitt,ods_shelf_life
+ msn_weather 通过date这个字段来控制增量
+ ods_sa_personuprice_501,ods_sa_personuprice_801 通过dstartdate来实现增量更新
+ ods_target_product_turnover 通过sale_date来实现增量
+ ods_ohem,ods_ousr,ods_oshp,ods_ocus,ods_oitm 通过docentry增量
+ ods_product_bill_other,ods_product_bill_shop 通过bill_date增量
+ ods_oitg,ods_itm1,ods_wsr1,ods_wsl1,ods_sck1,ods_sal1,ods_sal2,ods_owsr,ods_owsl,ods_osal 通过etl_time增量更新
+ ods_owor,ods_ostr 通过docdate增量更新
+ ods_str1 通过exitdate增量更新
+ ods_wor1 通过duedate增量更新

全量导入维表
--ods_oshp    成功的导入语句
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_oshp --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_oshp --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 

sqoop --options-file sqoop_option.txt --table ods_oshp --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_oshp --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1 

--ods_customer_501
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_customer_501 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_customer_501 --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1  

sqoop --options-file sqoop_option.txt  --table ods_customer_501 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_customer_501 --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1  

--ods_customer_801
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_customer_801 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_customer_801 --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

sqoop --options-file sqoop_option.txt   --table ods_customer_801 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_customer_801 --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

--ods_display
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_display --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_display --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1


sqoop --options-file sqoop_option.txt   --table ods_display --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_display --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

--ods_sa_personuprice_501
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_sa_personuprice_501 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_sa_personuprice_501 --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

sqoop --options-file sqoop_option.txt  --table ods_sa_personuprice_501 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_sa_personuprice_501 --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

--ods_sa_personuprice_801
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_sa_personuprice_801 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_sa_personuprice_801 --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

sqoop --options-file sqoop_option.txt  --table ods_sa_personuprice_801 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_sa_personuprice_801 --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

--ods_shelf_life
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_shelf_life --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_shelf_life --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

sqoop --options-file sqoop_option.txt   --table ods_shelf_life --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_shelf_life --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

--ods_oitm
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_oitm --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_oitm --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

sqoop --options-file sqoop_option.txt    --table ods_oitm --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_oitm --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

--ods_ousr
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_ousr --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_ousr --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

进阶版:
sqoop --options-file sqoop_option.txt --table ods_ousr --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_ousr --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1


-- ocus
sqoop  --options-file sqoop_option.txt --table ods_ocus --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_ocus --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

--ods_ohem
sqoop  --options-file sqoop_option.txt --table ods_ohem --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_ohem --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1




# 有问题,暂时全量更新
**--ods_ocus
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_ocus --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_ocus --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1**

sqoop --options-file sqoop_option.txt  --table ods_ocus --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_ocus --check-column date  --incremental append --last-value "2018-12-22"     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1


--check-column date  --incremental append --last-value "2018-12-22"      


增量导入成功的例子
sqoop --options-file sqoop_option.txt  --table test_append --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table test_append --check-column sale_date  --incremental append --last-value '2018-12-25'   --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema test


sqoop --options-file sqoop_option.txt  --table test_append --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table test_append --check-column etl_time  --incremental append --last-value '2018-12-25 18:50:07'   --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema test


增量导入

--msn_weather
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table msn_weather --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table msn_weather --check-column date  --incremental append --last-value "2018-12-21"  --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

sqoop --options-file sqoop_option.txt  --table msn_weather --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table msn_weather --check-column date  --incremental append --last-value "2018-12-22"     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_oitg
sqoop --options-file sqoop_option.txt  --table ods_oitg --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_oitg --check-column etl_time  --incremental append --last-value '2018-08-24 03:49:37 '   --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_oitt
sqoop --options-file sqoop_option.txt  --table ods_oitt --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_oitt --check-column etl_time  --incremental append --last-value '2018-08-24 03:47:50'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_osal 
sqoop --options-file sqoop_option.txt  --table ods_osal --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_osal --check-column etl_time  --incremental append --last-value '2018-12-14 03:47:51'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_ostr
sqoop --options-file sqoop_option.txt  --table ods_ostr --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_ostr --check-column docdate  --incremental append --last-value '2018-12-12 23:33:51'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_owor
sqoop --options-file sqoop_option.txt  --table ods_owor --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_owor --check-column docdate  --incremental append --last-value '2018-12-12 18:52:20'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_owsl
sqoop --options-file sqoop_option.txt  --table ods_owsl --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_owsl --check-column etl_time  --incremental append --last-value '2018-12-13 03:51:19'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_owsr
sqoop --options-file sqoop_option.txt  --table ods_owsr --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_owsr --check-column etl_time  --incremental append --last-value '2018-12-13 03:51:55'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_product_bill_other
sqoop --options-file sqoop_option.txt  --table ods_product_bill_other --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_product_bill_other --check-column bill_date  --incremental append --last-value '2018-12-24'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

--ods_product_bill_shop
sqoop --options-file sqoop_option.txt  --table ods_product_bill_shop --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_product_bill_shop --check-column bill_date  --incremental append --last-value '2018-12-24'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1


--ods_sal1
sqoop --options-file sqoop_option.txt  --table ods_sal1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_sal1 --check-column etl_time  --incremental append --last-value '2018-12-14 03:49:43'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_sal2
sqoop --options-file sqoop_option.txt  --table ods_sal2 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_sal2 --check-column etl_time  --incremental append --last-value '2018-12-13 03:51:14'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_sck1
sqoop --options-file sqoop_option.txt  --table ods_sck1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_sck1 --check-column etl_time  --incremental append --last-value '2018-12-14 03:51:36'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_str1
sqoop --options-file sqoop_option.txt  --table ods_str1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_str1 --check-column exitdate  --incremental append --last-value '2018-12-13 00:01:00'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_target_product_turnover
sqoop --options-file sqoop_option.txt  --table ods_target_product_turnover --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_target_product_turnover --check-column sale_date  --incremental append --last-value '2018-09-24'    --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods1

--ods_wor1
sqoop --options-file sqoop_option.txt  --table ods_wor1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_wor1 --check-column duedate  --incremental append --last-value '2018-12-12 18:52:26'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods


--ods_wsl1
sqoop --options-file sqoop_option.txt  --table ods_wsl1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_wsl1 --check-column etl_time  --incremental append --last-value '2018-12-14 03:50:34'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_wsr1
sqoop --options-file sqoop_option.txt  --table ods_wsr1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_wsr1 --check-column etl_time  --incremental append --last-value  '2018-12-13 03:51:59'  --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods


--ods_itm1

sqoop --options-file sqoop_option.txt  --table ods_itm1 --hive-database  rbu_sxcp_ods_dev  --hive-import --hive-table ods_itm1 --check-column etl_time  --incremental append --last-value '2018-12-13 03:46:58'     --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods












--ods_sal2
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_sal2 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_sal2  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods

--ods_wor1
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_wor1 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_wor1  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods


--ods_wsr1
sqoop import --connect jdbc:postgresql://47.98.135.132:2345/gpdb --username gpadmin --password bigdata2018 --table ods_wsr1 --hive-database  rbu_sxcp_ods_dev  --delete-target-dir --hive-import --hive-table ods_wsr1  --hive-overwrite --null-string '\\N'  --null-non-string '\\N' --hive-drop-import-delims -m 1 -- --schema ods



