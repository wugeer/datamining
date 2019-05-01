# hive_to_gp_rst_product_bill
```
CREATE OR REPLACE FUNCTION rst.hive_to_gp_rst_product_bill()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
begin
	insert into rst.rst_product_bill
	(store_pk, product_pk,store_code, store_name, bill_date, arrive_date, product_type, product_code, product_name, measure_unit, min_qty, min_display_qty, price_sale, price_wholesale, optimal_purchase, planner_purchase, planner_director_purchase, safety_stock_qty, final_purchase, min_package_qty, modify_reason, modify_purchase, modify_reason_type, planner_modify_reason, planner_modify_reason_type, planner_director_modify_reason, planner_director_modify_reason_type, max_modify, etl_time, update_time, remark, extra1, extra2)
	SELECT store_pk, product_pk, store_code, store_name, bill_date, arrive_date, product_type, product_code, product_name, measure_unit, min_qty, min_display_qty, price_sale, price_wholesale, optimal_purchase, planner_purchase, planner_director_purchase, safety_stock_qty, final_purchase, min_package_qty, modify_reason, modify_purchase, modify_reason_type, planner_modify_reason, planner_modify_reason_type, planner_director_modify_reason, planner_director_modify_reason_type, max_modify, etl_time, update_time, remark, extra1, extra2
	FROM rst.rst_product_bill_1;
return 1;
end;
$function$
;

```