# gp_product_bill_and_qty去重
```
drop table if exists tmp1;
create temp table tmp1 as 
SELECT 
row_number() over(partition by store_pk,product_pk,bill_date order by bill_date desc) date_id,
store_pk, 
product_pk, 
store_code, 
store_name, 
bill_date, 
arrive_date, 
product_type, product_code, product_name, measure_unit, min_qty, min_display_qty, price_sale, price_wholesale, optimal_purchase, planner_purchase, planner_director_purchase, safety_stock_qty, final_purchase, min_package_qty, modify_reason, modify_purchase, modify_reason_type, planner_modify_reason, planner_modify_reason_type, planner_director_modify_reason, planner_director_modify_reason_type, max_modify, etl_time, update_time, remark, extra1, extra2
FROM rst.rst_product_bill;


truncate table rst.rst_product_bill_1;
insert into rst.rst_product_bill_1
(select store_pk, 
product_pk, 
store_code, 
store_name, 
bill_date, 
arrive_date, 
product_type, product_code, product_name, measure_unit, min_qty, min_display_qty, price_sale, price_wholesale, optimal_purchase, planner_purchase, planner_director_purchase, safety_stock_qty, final_purchase, min_package_qty, modify_reason, modify_purchase, modify_reason_type, planner_modify_reason, planner_modify_reason_type, planner_director_modify_reason, planner_director_modify_reason_type, max_modify, etl_time, update_time, remark, extra1, extra2
 from tmp1 where date_id=1);

truncate table rst.rst_product_bill;
insert into rst.rst_product_bill
(store_pk, product_pk, store_code, store_name, bill_date, arrive_date, product_type, product_code, product_name, measure_unit, min_qty, min_display_qty, price_sale, price_wholesale, optimal_purchase, planner_purchase, planner_director_purchase, safety_stock_qty, final_purchase, min_package_qty, modify_reason, modify_purchase, modify_reason_type, planner_modify_reason, planner_modify_reason_type, planner_director_modify_reason, planner_director_modify_reason_type, max_modify, etl_time, update_time, remark, extra1, extra2)
SELECT store_pk, product_pk, store_code, store_name, bill_date, arrive_date, product_type, product_code, product_name, measure_unit, min_qty, min_display_qty, price_sale, price_wholesale, optimal_purchase, planner_purchase, planner_director_purchase, safety_stock_qty, final_purchase, min_package_qty, modify_reason, modify_purchase, modify_reason_type, planner_modify_reason, planner_modify_reason_type, planner_director_modify_reason, planner_director_modify_reason_type, max_modify, etl_time, update_time, remark, extra1, extra2
FROM rst.rst_product_bill_1;



drop table if exists tmp1;
create temp table tmp1 as 
SELECT 
row_number() over(partition by store_pk,product_pk,bill_date order by bill_date desc) date_id,
store_pk, product_pk, store_code, store_name, bill_date, arrive_date, product_type, product_code, product_name, d1_start_qty, d1_end_qty, d1_sale_predict, d1_loss_qty, d1_in_qty, d2_in_qty, d2_start_qty, d2_end_qty, d2_sale_predict, d2_loss_qty, d3_end_qty, d3_start_qty, d3_in_qty, d3_sale_predict, d3_loss_qty, remark, extra1, extra2, etl_time, update_time
FROM rst.rst_product_qty;

truncate table rst.rst_product_qty_1;
insert into rst.rst_product_qty_1
(select store_pk, product_pk, store_code, store_name, bill_date, arrive_date, product_type, product_code, product_name, d1_start_qty, d1_end_qty, d1_sale_predict, d1_loss_qty, d1_in_qty, d2_in_qty, d2_start_qty, d2_end_qty, d2_sale_predict, d2_loss_qty, d3_end_qty, d3_start_qty, d3_in_qty, d3_sale_predict, d3_loss_qty, remark, extra1, extra2, etl_time, update_time
from tmp1 where date_id=1);

truncate table rst.rst_product_qty;
insert into rst.rst_product_qty
(store_pk, product_pk, store_code, store_name, bill_date, arrive_date, product_type, product_code, product_name, d1_start_qty, d1_end_qty, d1_sale_predict, d1_loss_qty, d1_in_qty, d2_in_qty, d2_start_qty, d2_end_qty, d2_sale_predict, d2_loss_qty, d3_end_qty, d3_start_qty, d3_in_qty, d3_sale_predict, d3_loss_qty, remark, extra1, extra2, etl_time, update_time)
SELECT store_pk, product_pk, store_code, store_name, bill_date, arrive_date, product_type, product_code, product_name, d1_start_qty, d1_end_qty, d1_sale_predict, d1_loss_qty, d1_in_qty, d2_in_qty, d2_start_qty, d2_end_qty, d2_sale_predict, d2_loss_qty, d3_end_qty, d3_start_qty, d3_in_qty, d3_sale_predict, d3_loss_qty, remark, extra1, extra2, etl_time, update_time
FROM rst.rst_product_qty_1;
```