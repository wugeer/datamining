# hive rst层到gp


-- rst_store
后端
sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail --username postgres --password lzsj1701 --table rst_store_1 --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table rst_store --m 16 -- --schema rst


测试库
sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail_test --username postgres --password lzsj1701 --table rst_store_1 --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table rst_store --m 16 -- --schema rst


-- rst_store_product
后端
sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail --username postgres --password lzsj1701 --table rst_store_product_tmp --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table  rst_store_product --m 16 -- --schema rst

测试库
sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail_test --username postgres --password lzsj1701 --table rst_store_product_tmp --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table  rst_store_product --m 16 -- --schema rst

-- rst_dim_product
后端
sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail --username postgres --password lzsj1701 --table rst_product_tmp --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table  rst_dim_product --m 16 -- --schema rst

测试库
sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail_test --username postgres --password lzsj1701 --table rst_product_tmp --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table  rst_dim_product --m 16 -- --schema rst


sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail --username postgres --password lzsj1701 --table rst_product_1 --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table rst_product --m 16 -- --schema rst


-- rst_product_bill
后端
sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail --username postgres --password lzsj1701 --table rst_product_bill_1 --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table rst_product_bill --m 16 -- --schema rst


测试库
sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail_test --username postgres --password lzsj1701 --table rst_product_bill_1 --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table rst_product_bill --m 16 -- --schema rst

--rst_product_qty_1
后端
sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail --username postgres --password lzsj1701 --table rst_product_qty_1 --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table rst_product_qty_1 --m 16 -- --schema rst

测试库
sqoop export  --connect jdbc:postgresql://192.168.200.201:5432/linezone_retail_test --username postgres --password lzsj1701 --table rst_product_qty_1 --hcatalog-database rbu_sxcp_rst_dev --hcatalog-table rst_product_qty_1 --m 16 -- --schema rst