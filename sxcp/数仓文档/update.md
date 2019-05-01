## 2019-01-09 晚上
1.下午做了turnover_feat和sale_feat的未来预测(之前忘记做了)

2.早上的时候更新了sale_predict

3.现在还需要从gp rst层那边抽过来的数据有门店分类表,新老品(视模型需求),...

4.已经确定了rst_product_qty的计算逻辑
```
store_code<=bill_order.store_code
product_code<=bill_order.s=product_code
bill_date<=bill_order.bill_date
arrive_date<=arrive_date
d1_start_qty<=b.start_qty  由loss_pre_end_qty和bill_order的门店商品code,bill_date关联,loss_pre_end_qty.sale_date=今天
...
d2_start_qty 还是由loss_pre_end_qty提供这些数据,关联条件类似上面的,不同的是sale_date=明天
..
d3_Start_qty 类似上面的
```
5.今天离开前,要确认各个脚本的执行顺序和是否加了Java_home设置,不要再出现数据导入失败和edw和edw_ai的数据无法跑出来的情况了

### 存在的问题
1. 今天跑的数据,模型报错了,还没找到原因,现在估计是turnover_feat和sale_feat日期不是连续的

2.turnover_feat缺的天数18-09-07,18-10-08~18-10-31,18-11-14~18-11-31,19-01-01~19-01-08

ps:store_sale_add没缺天数

3.sale_feat缺18-09-07~18-09-09,18-09-14~18-09-16,18-09-21~18-09-24,18-09-30~18-10-31,18-11-02~18-11-04,18-11-09~18-11-11,18-11-14~18-12-19,18-12-21~18-12-23,18-12-29~19-01-08,19-01-11~19-01-13


## 2019-01-10

1. 今天给on_sale_history这个表的min_display_qty做了补零操作
2. 发现rst_weather是每天更新的,结果导致之前很多天天气数据关联不到,现在开始每天全量更新gp的rst.rst_weather,加入ods_overwrite.sh这个脚本了
3. 后面要先将模型跑的sale_predict先append到之前的sale_predict那个表,再用今天跑的销补的数据更新那个sale_predict相关的字段
4. 今天重新更新了七天销量排名,给每天在售商品做七天销量排名
5. 更新turnover_feat和sale_feat之前的case

## 2019-01-11
1. 断货率:判断当日断货,若日末库存<最小陈列量或者安全库存,用最后一笔销售时间和断货截止时间比较;若早于,则认为断货;否则不断货;断货截止时间可用门店销售金额比例计算,也可人工指定到门店级,品类级,甚至sku级

2. 报损率:期间sku报损金额/sku销售额

3. 盘点准确率:期间sku盘点准确天数/期间sku日末库存天数

4. 销售额:期间sku销售额

## 2019-01-13 
1.下午turnover_feat,sale_feat,digital_warehouse全抽到rbu_sxcp_tmp这个数据库下.并写到ai层了对应的表名为turnover_feat,sale_feat,digital_warehouse
2. 求周数时需要求出该周的周四,再求是第几周
3. 每天跑ai层之前,先将turnover_feat,sale_feat,sale_add_sum的case更新为最新的case
## 2019-01-14
1. sqoop --options-file sqoop_option.txt --query "SELECT docentry, docnum, billno, shpentry, exitdate, memo, etl_time,substring(exitdate::varchar,1, 10) sale_date
FROM ods.ods_osal WHERE exitdate>'2019-01-10 00:00:00' and \$CONDITIONS" --hive-database  rbu_sxcp_ods_dev --hive-table ods_osal_test --null-string '\\N'  --null-non-string '\\N' -m 1  --create-hive-table --hive-drop-import-delims  --hive-overwrite --hive-import --hive-partition-key sale_date --hive-partition-value VALUE --target-dir /liufuya/edw.db/ods_osal_test --delete-target-dir -- --schema ods
2. hive启动动态分区
SET hive.exec.dynamic.partition=true;  
SET hive.exec.dynamic.partition.mode=nonstrict; 
SET hive.exec.max.dynamic.partitions.pernode=1000;
3. sqoop导数据卡在xecuting SQL statement: SELECT t.* FROM "ods1"."rst_product_arrive" AS t LIMIT 1这一行,方法:减小这个表的数据量

## 2019-01-15
1. 模型在207上面写入数据后,用hive可以读出来,但是spark sql读不出来,现在还是没解决
2. hive启动报io异常没有那个权限,解决方法:修改为/tmp/你的用户名 这个文件夹的用户组为你的用户,如果还不行,将权限给为777试试
## 2019-01-16
1. sale_add_sum这个脚本跑了两个多小时还没跑完,原因未知
2. 在插入check_daily_stock数据时,居然有重复的数据