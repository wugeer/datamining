# shop_code_change_nerw
```
CREATE OR REPLACE FUNCTION rst.p_rst_shop_code_change_new(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--shelf_life更换
delete from rst.rst_shelf_life t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_shelf_life
(
	shop_code,
	product_code,
	warranty,
	shelf_life
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.warranty,
	t1.shelf_life
from rst.rst_shelf_life t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;


--product_display更换
delete from rst.rst_product_display t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_display
(
	shop_code,
	product_code,
	min_display_qty
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.min_display_qty
from rst.rst_product_display t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;


--price更换
delete from rst.rst_price t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_price
(
	shop_code,
	product_code,
	price_sale,
	price_wholesale
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.price_sale,
	t1.price_wholesale
from rst.rst_price t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;


--shop_test更换
update rst.rst_shop_test t1 
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--inventory更新
update rst.rst_inventory t1 
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--sale_supply_sum更新
update rst.rst_sale_supply_sum t1 
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--sale_supply_avg_copy更新
update rst.rst_sale_supply_avg_copy t1 
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--turnover_feat_sum更新
update rst.rst_turnover_feat_sum t1 
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--turnover_feat_avg更新
update rst.rst_turnover_feat_avg t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--turnover_feat更新
update rst.rst_turnover_feat t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--turnover_predict更新
update rst.rst_turnover_predict t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--sale_feat更新
update rst.rst_sale_feat t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--sale_predict更新
update rst.rst_sale_predict t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_predict_type更新
update rst.rst_product_predict_type t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--digital_warehouse更新
update rst.rst_digital_warehouse t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--order更新
update rst.rst_order t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_bill_model更新
update rst.rst_product_bill_model t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_bill_shop更新
update rst.rst_product_bill_shop t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_bill_other更新
update rst.rst_product_bill_other t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_arrive更新
update rst.rst_product_arrive t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--shop_date_case更新
update rst.rst_shop_date_case t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--stock更新
update rst.rst_stock t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--pos更新
update rst.rst_pos t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--shop_cluster更新
update rst.shop_cluster t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--off_product更新
update rst.rst_off_product t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_cost_index更新
update rst.rst_cost_index t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_rate_of_shorts更新
update rst.rst_rate_of_shorts t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_shop_pause_history更新
update rst.rst_shop_pause_history t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_target_product_turnover更新
update rst.rst_target_product_turnover t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_target_turnover更新
update rst.rst_target_turnover t1
set shop_code=t2.new_shop_code
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--========================================================================================
--management_analysis更新
update rst.rst_management_analysis t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_sale_day更新
update rst.rst_product_sale_day t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_sale_week
update rst.rst_product_sale_week t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_sale_month
update rst.rst_product_sale_month t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_sale_year
update rst.rst_product_sale_year t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_target_monitor
update rst.rst_product_target_monitor t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_target_monitor_week
update rst.rst_product_target_monitor_week t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_target_monitor_month
update rst.rst_product_target_monitor_month t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_target_monitor_year
update rst.rst_product_target_monitor_year t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_accept_shorts_day=========================================================
update analysis.analysis_accept_shorts_day t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_accept_shorts_month
update analysis.analysis_accept_shorts_month t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_accept_shorts_week
update analysis.analysis_accept_shorts_week t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_check_error_day
update analysis.analysis_check_error_day t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_check_error_detail
update analysis.analysis_check_error_detail t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_check_error_week
update analysis.analysis_check_error_week t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_cost_day
update analysis.analysis_cost_day t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_cost_detail
update analysis.analysis_cost_detail t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_cost_month
update analysis.analysis_cost_month t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_cost_week
update analysis.analysis_cost_week t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_main_index_day
update analysis.analysis_main_index_day t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_main_index_history
update analysis.analysis_main_index_history t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_main_index_month
update analysis.analysis_main_index_month t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_main_index_week
update analysis.analysis_main_index_week t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_model_accept
update analysis.analysis_model_accept t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_model_accept_type
update analysis.analysis_model_accept_type t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_model_better_percent
update analysis.analysis_model_better_percent t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_shop_shorts_day
update analysis.analysis_shop_shorts_day t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_shop_shorts_month
update analysis.analysis_shop_shorts_month t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_shop_shorts_week
update analysis.analysis_shop_shorts_week t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_shorts_detail
update analysis.analysis_shorts_detail t1
set shop_code=t2.new_shop_code,
	shop_name=t2.new_shop_name
from rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;



	RETURN 1;
END

$function$
;

```