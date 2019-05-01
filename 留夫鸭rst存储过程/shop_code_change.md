# shop_code_change
```
CREATE OR REPLACE FUNCTION rst.p_rst_shop_code_change(date)
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
	shelf_life,
	best_life
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.warranty,
	t1.shelf_life,
	t1.best_life
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


/*
--shop补数据
insert into rst.rst_shop
(
	shop_code,
	shop_name,
	shop_type,
	shop_class,
	price_list,
	address,
	area,
	province,
	city,
	county_district,
	distribute_route,
	is_valid,
	district_manager_code,
	district_manager_name,
	district_director_code,
	district_director_name,
	order_cycle,
	start_date,
	phone_number,
	shop_opening_time,
	shop_closed_time
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.shop_type,
	t1.shop_class,
	t1.price_list,
	t1.address,
	t1.area,
	t1.province,
	t1.city,
	t1.county_district,
	t1.distribute_route,
	t1.is_valid,
	t1.district_manager_code,
	t1.district_manager_name,
	t1.district_director_code,
	t1.district_director_name,
	t1.order_cycle,
	t1.start_date,
	t1.phone_number,
	t1.shop_opening_time,
	t1.shop_closed_time
from rst.rst_shop t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;
*/


--shop_test更换
delete from rst.rst_shop_test t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_shop_test 
(
	shop_code
)
select 
	t2.new_shop_code shop_code
from rst.rst_shop_test t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_shop_test t1
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_shop_pos_inaccurate更换
delete from rst.rst_shop_pos_inaccurate t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_shop_pos_inaccurate 
(
	shop_code,
	shop_name
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name
from rst.rst_shop_pos_inaccurate t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_shop_pos_inaccurate t1
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst_product_onsale_history更换
delete from rst.rst_product_onsale_history t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_onsale_history
(
	shop_code, 
	shop_name, 
	product_code, 
	product_name, 
	product_type, 
	sale_date, 
	price_sale, 
	price_wholesale, 
	shelf_life, 
	measure_unit, 
	min_qty, 
	min_display_qty, 
	best_life,
	is_main
)
select 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.product_code, 
	t1.product_name, 
	t1.product_type, 
	t1.sale_date, 
	t1.price_sale, 
	t1.price_wholesale, 
	t1.shelf_life, 
	t1.measure_unit, 
	t1.min_qty, 
	t1.min_display_qty, 
	t1.best_life,
	t1.is_main
from rst.rst_product_onsale_history t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_onsale_history t1
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--inventory补数据
delete from rst.rst_inventory t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_inventory 
(
	shop_code,
	product_code,
	check_date,
	begin_qty,
	in_qty,
	group_buying_qty,
	sale_qty,
	sale_supply_qty,
	loss_qty,
	end_qty,
	sale_total_qty,
	inferential_end_qty,
	taste_give_qty
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.check_date,
	t1.begin_qty,
	t1.in_qty,
	t1.group_buying_qty,
	t1.sale_qty,
	t1.sale_supply_qty,
	t1.loss_qty,
	t1.end_qty,
	t1.sale_total_qty,
	t1.inferential_end_qty,
	t1.taste_give_qty
from rst.rst_inventory t1
inner join rst.rst_shop_code_change t2
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_inventory t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--sale_supply_sum补数据
delete from rst.rst_sale_supply_sum t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_sale_supply_sum 
(
	shop_code,
	product_code,
	date_case,
	sale_date,
	max_min,
	sum_3_sale,
	sum_3a_sale,
	sum_4_sale,
	sum_4a_sale,
	sum_5_sale,
	sum_5a_sale,
	sum_6_sale,
	sum_6a_sale,
	sum_7_sale,
	sum_7a_sale,
	sum_8_sale,
	sum_8a_sale,
	sum_9_sale,
	sum_9a_sale,
	sum_10_sale,
	sum_10a_sale,
	sum_11_sale,
	sum_11a_sale,
	sum_12_sale,
	sum_12a_sale,
	sum_13_sale,
	sum_13a_sale,
	sum_14_sale,
	sum_14a_sale,
	sum_15_sale,
	sum_15a_sale,
	sum_16_sale,
	sum_16a_sale,
	sum_17_sale,
	sum_17a_sale,
	sum_18_sale,
	sum_18a_sale,
	sum_19_sale,
	sum_19a_sale,
	sum_20_sale,
	sum_20a_sale,
	sum_21_sale,
	sum_21a_sale,
	sum_22_sale,
	sum_22a_sale,
	sum_23_sale,
	sum_23a_sale,
	sum_0_sale,
	sum_0a_sale,
	sum_1_sale,
	sum_1a_sale,
	sum_2_sale,
	sum_2a_sale,
	end_qty
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.date_case,
	t1.sale_date,
	t1.max_min,
	t1.sum_3_sale,
	t1.sum_3a_sale,
	t1.sum_4_sale,
	t1.sum_4a_sale,
	t1.sum_5_sale,
	t1.sum_5a_sale,
	t1.sum_6_sale,
	t1.sum_6a_sale,
	t1.sum_7_sale,
	t1.sum_7a_sale,
	t1.sum_8_sale,
	t1.sum_8a_sale,
	t1.sum_9_sale,
	t1.sum_9a_sale,
	t1.sum_10_sale,
	t1.sum_10a_sale,
	t1.sum_11_sale,
	t1.sum_11a_sale,
	t1.sum_12_sale,
	t1.sum_12a_sale,
	t1.sum_13_sale,
	t1.sum_13a_sale,
	t1.sum_14_sale,
	t1.sum_14a_sale,
	t1.sum_15_sale,
	t1.sum_15a_sale,
	t1.sum_16_sale,
	t1.sum_16a_sale,
	t1.sum_17_sale,
	t1.sum_17a_sale,
	t1.sum_18_sale,
	t1.sum_18a_sale,
	t1.sum_19_sale,
	t1.sum_19a_sale,
	t1.sum_20_sale,
	t1.sum_20a_sale,
	t1.sum_21_sale,
	t1.sum_21a_sale,
	t1.sum_22_sale,
	t1.sum_22a_sale,
	t1.sum_23_sale,
	t1.sum_23a_sale,
	t1.sum_0_sale,
	t1.sum_0a_sale,
	t1.sum_1_sale,
	t1.sum_1a_sale,
	t1.sum_2_sale,
	t1.sum_2a_sale,
	t1.end_qty
from rst.rst_sale_supply_sum t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_sale_supply_sum t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--sale_supply_avg_copy补数据
delete from rst.rst_sale_supply_avg_copy t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_sale_supply_avg_copy
(
	shop_code,
	product_code,
	date_case,
	avg_3_sale,
	avg_3a_sale,
	avg_4_sale,
	avg_4a_sale,
	avg_5_sale,
	avg_5a_sale,
	avg_6_sale,
	avg_6a_sale,
	avg_7_sale,
	avg_7a_sale,
	avg_8_sale,
	avg_8a_sale,
	avg_9_sale,
	avg_9a_sale,
	avg_10_sale,
	avg_10a_sale,
	avg_11_sale,
	avg_11a_sale,
	avg_12_sale,
	avg_12a_sale,
	avg_13_sale,
	avg_13a_sale,
	avg_14_sale,
	avg_14a_sale,
	avg_15_sale,
	avg_15a_sale,
	avg_16_sale,
	avg_16a_sale,
	avg_17_sale,
	avg_17a_sale,
	avg_18_sale,
	avg_18a_sale,
	avg_19_sale,
	avg_19a_sale,
	avg_20_sale,
	avg_20a_sale,
	avg_21_sale,
	avg_21a_sale,
	avg_22_sale,
	avg_22a_sale,
	avg_23_sale,
	avg_23a_sale,
	avg_0_sale,
	avg_0a_sale,
	avg_1_sale,
	avg_1a_sale,
	avg_2_sale,
	avg_2a_sale,
	sale_date
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.date_case,
	t1.avg_3_sale,
	t1.avg_3a_sale,
	t1.avg_4_sale,
	t1.avg_4a_sale,
	t1.avg_5_sale,
	t1.avg_5a_sale,
	t1.avg_6_sale,
	t1.avg_6a_sale,
	t1.avg_7_sale,
	t1.avg_7a_sale,
	t1.avg_8_sale,
	t1.avg_8a_sale,
	t1.avg_9_sale,
	t1.avg_9a_sale,
	t1.avg_10_sale,
	t1.avg_10a_sale,
	t1.avg_11_sale,
	t1.avg_11a_sale,
	t1.avg_12_sale,
	t1.avg_12a_sale,
	t1.avg_13_sale,
	t1.avg_13a_sale,
	t1.avg_14_sale,
	t1.avg_14a_sale,
	t1.avg_15_sale,
	t1.avg_15a_sale,
	t1.avg_16_sale,
	t1.avg_16a_sale,
	t1.avg_17_sale,
	t1.avg_17a_sale,
	t1.avg_18_sale,
	t1.avg_18a_sale,
	t1.avg_19_sale,
	t1.avg_19a_sale,
	t1.avg_20_sale,
	t1.avg_20a_sale,
	t1.avg_21_sale,
	t1.avg_21a_sale,
	t1.avg_22_sale,
	t1.avg_22a_sale,
	t1.avg_23_sale,
	t1.avg_23a_sale,
	t1.avg_0_sale,
	t1.avg_0a_sale,
	t1.avg_1_sale,
	t1.avg_1a_sale,
	t1.avg_2_sale,
	t1.avg_2a_sale,
	t1.sale_date
from rst.rst_sale_supply_avg_copy t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_sale_supply_avg_copy t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--turnover_feat_sum补数据
delete from rst.rst_turnover_feat_sum t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_turnover_feat_sum
(
	shop_code,
	sale_date,
	time_3_turnover,
	time_4_turnover,
	time_5_turnover,
	time_6_turnover,
	time_7_turnover,
	time_8_turnover,
	time_9_turnover,
	time_10_turnover,
	time_11_turnover,
	time_12_turnover,
	time_13_turnover,
	time_14_turnover,
	time_15_turnover,
	time_16_turnover,
	time_17_turnover,
	time_18_turnover,
	time_19_turnover,
	time_20_turnover,
	time_21_turnover,
	time_22_turnover,
	time_23_turnover,
	time_0_turnover,
	time_1_turnover,
	time_2_turnover
)
select 
	t2.new_shop_code shop_code,
	t1.sale_date,
	t1.time_3_turnover,
	t1.time_4_turnover,
	t1.time_5_turnover,
	t1.time_6_turnover,
	t1.time_7_turnover,
	t1.time_8_turnover,
	t1.time_9_turnover,
	t1.time_10_turnover,
	t1.time_11_turnover,
	t1.time_12_turnover,
	t1.time_13_turnover,
	t1.time_14_turnover,
	t1.time_15_turnover,
	t1.time_16_turnover,
	t1.time_17_turnover,
	t1.time_18_turnover,
	t1.time_19_turnover,
	t1.time_20_turnover,
	t1.time_21_turnover,
	t1.time_22_turnover,
	t1.time_23_turnover,
	t1.time_0_turnover,
	t1.time_1_turnover,
	t1.time_2_turnover
from rst.rst_turnover_feat_sum t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_turnover_feat_sum t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--turnover_feat_avg补数据
delete from rst.rst_turnover_feat_avg t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_turnover_feat_avg
(
	shop_code,
	per_3_turnover,
	per_4_turnover,
	per_5_turnover,
	per_6_turnover,
	per_7_turnover,
	per_8_turnover,
	per_9_turnover,
	per_10_turnover,
	per_11_turnover,
	per_12_turnover,
	per_13_turnover,
	per_14_turnover,
	per_15_turnover,
	per_16_turnover,
	per_17_turnover,
	per_18_turnover,
	per_19_turnover,
	per_20_turnover,
	per_21_turnover,
	per_22_turnover,
	per_23_turnover,
	per_0_turnover,
	per_1_turnover,
	per_2_turnover
)
select 
	t2.new_shop_code shop_code,
	t1.per_3_turnover,
	t1.per_4_turnover,
	t1.per_5_turnover,
	t1.per_6_turnover,
	t1.per_7_turnover,
	t1.per_8_turnover,
	t1.per_9_turnover,
	t1.per_10_turnover,
	t1.per_11_turnover,
	t1.per_12_turnover,
	t1.per_13_turnover,
	t1.per_14_turnover,
	t1.per_15_turnover,
	t1.per_16_turnover,
	t1.per_17_turnover,
	t1.per_18_turnover,
	t1.per_19_turnover,
	t1.per_20_turnover,
	t1.per_21_turnover,
	t1.per_22_turnover,
	t1.per_23_turnover,
	t1.per_0_turnover,
	t1.per_1_turnover,
	t1.per_2_turnover
from rst.rst_turnover_feat_avg t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_turnover_feat_avg t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--turnover_feat补数据
delete from rst.rst_turnover_feat t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_turnover_feat 
(
	shop_code,
	sale_date,
	date_case,
	turnover,
	pre,
	"temp",
	recent_3,
	recent_7
)
select 
	t2.new_shop_code shop_code,
	t1.sale_date,
	t1.date_case,
	t1.turnover,
	t1.pre,
	t1."temp",
	t1.recent_3,
	t1.recent_7
from rst.rst_turnover_feat t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_turnover_feat t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--turnover_predict补数据
delete from rst.rst_turnover_predict t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_turnover_predict 
(
	shop_code,
	sale_date,
	turnover,
	d1,
	d2,
	d3,
	d4,
	d5,
	d6,
	d7,
	mae,
	r2
)
select 
	t2.new_shop_code shop_code,
	t1.sale_date,
	t1.turnover,
	t1.d1,
	t1.d2,
	t1.d3,
	t1.d4,
	t1.d5,
	t1.d6,
	t1.d7,
	t1.mae,
	t1.r2
from rst.rst_turnover_predict t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_turnover_predict t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--sale_feat补数据
delete from rst.rst_sale_feat t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_sale_feat 
(
	shop_code,
	product_code,
	sale_date,
	date_case,
	turnover,
	temp,
	qty,
	recent_3,
	recent_7
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.sale_date,
	t1.date_case,
	t1.turnover,
	t1.temp,
	t1.qty,
	t1.recent_3,
	t1.recent_7
from rst.rst_sale_feat t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_sale_feat t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--sale_predict补数据
delete from rst.rst_sale_predict t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_sale_predict 
(
	shop_code,
	product_code,
	sale_date,
	qty,
	d1,
	d2,
	d3,
	d4,
	d5,
	d6,
	d7,
	error1,
	error2,
	error3,
	error4,
	error5,
	error6,
	error7
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.sale_date,
	t1.qty,
	t1.d1,
	t1.d2,
	t1.d3,
	t1.d4,
	t1.d5,
	t1.d6,
	t1.d7,
	t1.error1,
	t1.error2,
	t1.error3,
	t1.error4,
	t1.error5,
	t1.error6,
	t1.error7
from rst.rst_sale_predict t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code 
where t2.change_date=$1
;

delete from rst.rst_sale_predict t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_predict_type补数据
delete from rst.rst_product_predict_type t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_predict_type 
(
	shop_code,
	product_code,
	new_old_type_yesterday,
	new_old_type,
	predict_type,
	sale_date
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.new_old_type_yesterday,
	t1.new_old_type,
	t1.predict_type,
	t1.sale_date
from rst.rst_product_predict_type t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_predict_type t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--digital_warehouse补数据
delete from rst.rst_digital_warehouse t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_digital_warehouse
(
	shop_code,
	product_code,
	check_date,
	shelf_life,
	end_qty,
	digital_warehouse
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.check_date,
	t1.shelf_life,
	t1.end_qty,
	t1.digital_warehouse
from rst.rst_digital_warehouse t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_digital_warehouse t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--order补数据
delete from rst.rst_order t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_order
(
	doc_no,
	shop_code,
	product_code,
	qty,
	amt,
	order_date,
	order_time,
	order_type,
	remark
)
select 
	t1.doc_no,
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.qty,
	t1.amt,
	t1.order_date,
	t1.order_time,
	t1.order_type,
	t1.remark
from rst.rst_order t1 
inner join rst.rst_shop_code_change t2
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_order t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_bill_model补数据
delete from rst.rst_product_bill_model t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_bill_model 
(
	shop_code,
	shop_name,
	bill_date,
	arrive_date,
	product_type,
	product_code,
	product_name,
	measure_unit,
	min_qty,
	min_display_qty,
	price_sale,
	price_wholesale,
	d0_end_qty,
	d1_in_qty,
	d2_in_qty,
	d2_end_qty_predict,
	optimal_purchase,
	final_purchase,
	d3_sale_predict,
	create_time
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.bill_date,
	t1.arrive_date,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.measure_unit,
	t1.min_qty,
	t1.min_display_qty,
	t1.price_sale,
	t1.price_wholesale,
	t1.d0_end_qty,
	t1.d1_in_qty,
	t1.d2_in_qty,
	t1.d2_end_qty_predict,
	t1.optimal_purchase,
	t1.final_purchase,
	t1.d3_sale_predict,
	t1.create_time
from rst.rst_product_bill_model t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_bill_model t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_bill_shop补数据
delete from rst.rst_product_bill_shop t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_bill_shop 
(
	shop_code,
	shop_name,
	bill_date,
	arrive_date,
	product_type,
	product_code,
	product_name,
	measure_unit,
	min_qty,
	min_display_qty,
	price_sale,
	price_wholesale,
	d0_end_qty,
	d1_in_qty,
	d2_in_qty,
	d2_end_qty_predict,
	optimal_purchase,
	final_purchase,
	d3_sale_predict,
	create_time,
	update_time,
	feedback_reason,
	del_flag,
	off_flag,
	shop_purchase,
	operations_purchase,
	district_manager_code,
	district_manager_name,
	district_director_code,
	district_director_name,
	supply_purchase
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.bill_date,
	t1.arrive_date,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.measure_unit,
	t1.min_qty,
	t1.min_display_qty,
	t1.price_sale,
	t1.price_wholesale,
	t1.d0_end_qty,
	t1.d1_in_qty,
	t1.d2_in_qty,
	t1.d2_end_qty_predict,
	t1.optimal_purchase,
	t1.final_purchase,
	t1.d3_sale_predict,
	t1.create_time,
	t1.update_time,
	t1.feedback_reason,
	t1.del_flag,
	t1.off_flag,
	t1.shop_purchase,
	t1.operations_purchase,
	t1.district_manager_code,
	t1.district_manager_name,
	t1.district_director_code,
	t1.district_director_name,
	t1.supply_purchase
from rst.rst_product_bill_shop t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_bill_shop t1
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_bill_other补数据
delete from rst.rst_product_bill_other t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_bill_other 
(
	goods_type,
	shop_code,
	shop_name,
	shop_type,
	bill_date,
	arr_date,
	product_type,
	product_code,
	product_name,
	measure_unit,
	min_qty,
	night_qty,
	price_sale,
	price_wholesale,
	shop_qty,
	area_qty,
	fin_qty,
	create_time
)
select 
	t1.goods_type,
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.shop_type,
	t1.bill_date,
	t1.arr_date,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.measure_unit,
	t1.min_qty,
	t1.night_qty,
	t1.price_sale,
	t1.price_wholesale,
	t1.shop_qty,
	t1.area_qty,
	t1.fin_qty,
	t1.create_time
from rst.rst_product_bill_other t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_bill_other t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_arrive补数据
delete from rst.rst_product_arrive t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_arrive
(
	shop_code, 
	product_code, 
	arrive_date, 
	arrive_qty, 
	supply_qty, 
	final_qty
)
select 
	t2.new_shop_code shop_code, 
	t1.product_code, 
	t1.arrive_date, 
	t1.arrive_qty, 
	t1.supply_qty, 
	t1.final_qty
from rst.rst_product_arrive t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_arrive t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst_product_new补数据
delete from rst.rst_product_new t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_new
(
	shop_code, 
	shop_name, 
	shop_type, 
	product_code, 
	product_name, 
	product_type, 
	putaway_date, 
	putaway_time
)
select 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.product_code, 
	t1.product_name, 
	t1.product_type, 
	t1.putaway_date, 
	t1.putaway_time
from rst.rst_product_new t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_new t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--shop_date_case补数据
delete from rst.rst_shop_date_case t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_shop_date_case
(
	shop_code,
	"date",
	holiday,
	week,
	"case"
)
select 
	t2.new_shop_code shop_code,
	t1."date",
	t1.holiday,
	t1.week,
	t1."case"
from rst.rst_shop_date_case t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_shop_date_case t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--stock补数据
delete from rst.rst_stock t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_stock
(
	shop_code,
	product_code,
	qty,
	check_date
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.qty,
	t1.check_date
from rst.rst_stock t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_stock t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--pos补数据
delete from rst.rst_pos t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_pos
(
	shop_code,
	product_code,
	product_name,
	doc_no,
	sale_date,
	sale_time,
	sale_min,
	week,
	qty,
	amt_origin,
	amt,
	measure_unit
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.product_name,
	t1.doc_no,
	t1.sale_date,
	t1.sale_time,
	t1.sale_min,
	t1.week,
	t1.qty,
	t1.amt_origin,
	t1.amt,
	t1.measure_unit
from rst.rst_pos t1 
inner join rst.rst_shop_code_change t2
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_pos t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--shop_cluster补数据
delete from rst.shop_cluster t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.shop_cluster
(
	shop_code,
	cluster_index
)
select 
	t2.new_shop_code shop_code,
	t1.cluster_index
from rst.shop_cluster t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;


--off_product补数据
delete from rst.rst_off_product t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_off_product
(
	shop_code,
	product_code,
	product_name,
	reason,
	create_time,
	ext1,
	min_qty,
	price_sale
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.product_name,
	t1.reason,
	t1.create_time,
	t1.ext1,
	t1.min_qty,
	t1.price_sale
from rst.rst_off_product t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_off_product t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_cost_index补数据
delete from rst.rst_cost_index t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_cost_index
(
	shop_code,
	sale_date,
	sale_year,
	sale_week,
	sale_amt,
	taste_amt,
	d_taste_amt,
	dw_taste_amt,
	w_taste_amt,
	give_amt,
	d_give_amt,
	dw_give_amt,
	w_give_amt,
	loss_amt,
	d_loss_amt,
	dw_loss_amt,
	w_loss_amt,
	discount_amt,
	d_discount_amt,
	dw_discount_amt,
	w_discount_amt,
	sum_amt,
	d_sum_amt,
	dw_sum_amt,
	w_sum_amt
)
select 
	t2.new_shop_code shop_code,
	t1.sale_date,
	t1.sale_year,
	t1.sale_week,
	t1.sale_amt,
	t1.taste_amt,
	t1.d_taste_amt,
	t1.dw_taste_amt,
	t1.w_taste_amt,
	t1.give_amt,
	t1.d_give_amt,
	t1.dw_give_amt,
	t1.w_give_amt,
	t1.loss_amt,
	t1.d_loss_amt,
	t1.dw_loss_amt,
	t1.w_loss_amt,
	t1.discount_amt,
	t1.d_discount_amt,
	t1.dw_discount_amt,
	t1.w_discount_amt,
	t1.sum_amt,
	t1.d_sum_amt,
	t1.dw_sum_amt,
	t1.w_sum_amt
from rst.rst_cost_index t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_cost_index t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_rate_of_shorts补数据
delete from rst.rst_rate_of_shorts t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_rate_of_shorts
(
	shop_code,
	product_code,
	cut_off_time,
	is_turnover,
	rate
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.cut_off_time,
	t1.is_turnover,
	t1.rate
from rst.rst_rate_of_shorts t1 
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_rate_of_shorts t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_shop_pause_history补数据
delete from rst.rst_shop_pause_history t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_shop_pause_history
(
	shop_code,
	pause_reason,
	pause_time_point,
	resume_time_point,
	duration,
	create_user,
	update_time,
	create_time,
	resume_choice
)
select 
	t2.new_shop_code shop_code,
	t1.pause_reason,
	t1.pause_time_point,
	t1.resume_time_point,
	t1.duration,
	t1.create_user,
	t1.update_time,
	t1.create_time,
	t1.resume_choice
from rst.rst_shop_pause_history t1
inner join rst.rst_shop_code_change t2
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_shop_pause_history t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_target_product_turnover补数据
delete from rst.rst_target_product_turnover t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_target_product_turnover
(
	shop_code, 
	product_code, 
	sale_date, 
	target_product_turnover
)
select 
	t2.new_shop_code shop_code, 
	t1.product_code, 
	t1.sale_date, 
	t1.target_product_turnover
from rst.rst_target_product_turnover t1
inner join rst.rst_shop_code_change t2
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_target_product_turnover t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_target_turnover补数据
delete from rst.rst_target_turnover t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_target_turnover
(
	shop_code, 
	sale_date, 
	target_turnover
)
select 
	t2.new_shop_code shop_code, 
	t1.sale_date, 
	t1.target_turnover
from rst.rst_target_turnover t1
inner join rst.rst_shop_code_change t2
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_target_turnover t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_activity补数据
delete from rst.rst_activity t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_activity
(
	shop_code,
	product_code,
	sale_date
)
select 
	t2.new_shop_code shop_code, 
	t1.product_code, 
	t1.sale_date
from rst.rst_activity t1
inner join rst.rst_shop_code_change t2
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_activity t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_activity_curve补数据
delete from rst.rst_activity_curve t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_activity_curve
(
	shop_code,
	product_code,
	begin_date,
	end_date,
	before_qty,
	activity_percent,
	after_percent_1,
	after_percent_2,
	after_percent_3
)
select 
	t2.new_shop_code shop_code,
	t1.product_code,
	t1.begin_date,
	t1.end_date,
	t1.before_qty,
	t1.activity_percent,
	t1.after_percent_1,
	t1.after_percent_2,
	t1.after_percent_3
from rst.rst_activity_curve t1
inner join rst.rst_shop_code_change t2
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_activity_curve t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--rst.rst_product_short_analysis补数据
delete from rst.rst_product_short_analysis t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_short_analysis
(
	shop_code,
	shop_name,
	bill_date,
	arrive_date,
	product_type,
	product_code,
	product_name,
	shorts_time,
	check_status,
	computer_end_qty,
	d0_end_qty,
	d1_in_qty,
	d2_in_qty,
	d2_end_qty_predict,
	optimal_purchase,
	final_purchase,
	d3_sale_predict,
	actual_sale_qty,
	loss_qty,
	taste_qty,
	give_qty,
	allot_qty,
	d3_sale_actual
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.bill_date,
	t1.arrive_date,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.shorts_time,
	t1.check_status,
	t1.computer_end_qty,
	t1.d0_end_qty,
	t1.d1_in_qty,
	t1.d2_in_qty,
	t1.d2_end_qty_predict,
	t1.optimal_purchase,
	t1.final_purchase,
	t1.d3_sale_predict,
	t1.actual_sale_qty,
	t1.loss_qty,
	t1.taste_qty,
	t1.give_qty,
	t1.allot_qty,
	t1.d3_sale_actual
from rst.rst_product_short_analysis t1
inner join rst.rst_shop_code_change t2
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_short_analysis t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;

--========================================================================================
--management_analysis补数据
delete from rst.rst_management_analysis t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_management_analysis
(
	shop_code,
	shop_name,
	sale_date,
	sale_year,
	sale_week,
	sale_amt,
	customer_qty,
	customer_price,
	is_sale
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.sale_date,
	t1.sale_year,
	t1.sale_week,
	t1.sale_amt,
	t1.customer_qty,
	t1.customer_price,
	t1.is_sale
from rst.rst_management_analysis t1
inner join rst.rst_shop_code_change t2
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_management_analysis t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_sale_day补数据
delete from rst.rst_product_sale_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_sale_day
(
	shop_code,
	shop_name,
	product_type,
	product_code,
	product_name,
	sale_date,
	qty,
	one_ago_qty,
	two_ago_qty,
	three_ago_qty,
	four_ago_qty,
	five_ago_qty,
	six_ago_qty,
	com_qty,
	one_ago_com_qty,
	two_ago_com_qty,
	three_ago_com_qty,
	four_ago_com_qty,
	five_ago_com_qty,
	six_ago_com_qty,
	measure_unit
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.sale_date,
	t1.qty,
	t1.one_ago_qty,
	t1.two_ago_qty,
	t1.three_ago_qty,
	t1.four_ago_qty,
	t1.five_ago_qty,
	t1.six_ago_qty,
	t1.com_qty,
	t1.one_ago_com_qty,
	t1.two_ago_com_qty,
	t1.three_ago_com_qty,
	t1.four_ago_com_qty,
	t1.five_ago_com_qty,
	t1.six_ago_com_qty,
	t1.measure_unit
from rst.rst_product_sale_day t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_sale_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_sale_week
delete from rst.rst_product_sale_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_sale_week
(
	shop_code,
	shop_name,
	product_type,
	product_code,
	product_name,
	sale_year,
	sale_week,
	qty,
	one_ago_qty,
	two_ago_qty,
	three_ago_qty,
	four_ago_qty,
	five_ago_qty,
	six_ago_qty,
	measure_unit
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.sale_year,
	t1.sale_week,
	t1.qty,
	t1.one_ago_qty,
	t1.two_ago_qty,
	t1.three_ago_qty,
	t1.four_ago_qty,
	t1.five_ago_qty,
	t1.six_ago_qty,
	t1.measure_unit
from rst.rst_product_sale_week t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_sale_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_sale_month
delete from rst.rst_product_sale_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_sale_month
(
	shop_code,
	shop_name,
	product_type,
	product_code,
	product_name,
	sale_year,
	sale_month,
	qty,
	one_ago_qty,
	two_ago_qty,
	com_qty,
	one_ago_com_qty,
	two_ago_com_qty,
	measure_unit
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.sale_year,
	t1.sale_month,
	t1.qty,
	t1.one_ago_qty,
	t1.two_ago_qty,
	t1.com_qty,
	t1.one_ago_com_qty,
	t1.two_ago_com_qty,
	t1.measure_unit
from rst.rst_product_sale_month t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_sale_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_sale_year
delete from rst.rst_product_sale_year t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_sale_year
(
	shop_code,
	shop_name,
	product_type,
	product_code,
	product_name,
	sale_year,
	qty,
	com_qty,
	measure_unit 
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.sale_year,
	t1.qty,
	t1.com_qty,
	t1.measure_unit 
from rst.rst_product_sale_year t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_sale_year t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_target_monitor
delete from rst.rst_product_target_monitor t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_target_monitor 
(
	shop_code,
	shop_name,
	product_type,
	product_code,
	product_name,
	sale_date,
	sale_amt,
	wholesale_amt,
	loss_amt,
	taste_amt,
	give_amt,
	discount_amt,
	sum_cost_amt,
	customer_qty,
	customer_price
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.sale_date,
	t1.sale_amt,
	t1.wholesale_amt,
	t1.loss_amt,
	t1.taste_amt,
	t1.give_amt,
	t1.discount_amt,
	t1.sum_cost_amt,
	t1.customer_qty,
	t1.customer_price
from rst.rst_product_target_monitor t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_target_monitor t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_target_monitor_week
delete from rst.rst_product_target_monitor_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_target_monitor_week
(
	shop_code,
	shop_name,
	product_type,
	product_code,
	product_name,
	sale_year,
	sale_week,
	sale_amt,
	wholesale_amt,
	loss_amt,
	taste_amt,
	give_amt,
	discount_amt,
	sum_cost_amt,
	customer_qty,
	customer_price
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.sale_year,
	t1.sale_week,
	t1.sale_amt,
	t1.wholesale_amt,
	t1.loss_amt,
	t1.taste_amt,
	t1.give_amt,
	t1.discount_amt,
	t1.sum_cost_amt,
	t1.customer_qty,
	t1.customer_price
from rst.rst_product_target_monitor_week t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_target_monitor_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_target_monitor_month
delete from rst.rst_product_target_monitor_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_target_monitor_month
(
	shop_code,
	shop_name,
	product_type,
	product_code,
	product_name ,
	sale_year,
	sale_month,
	sale_amt,
	wholesale_amt,
	loss_amt,
	taste_amt,
	give_amt,
	discount_amt,
	sum_cost_amt,
	customer_qty,
	customer_price
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.sale_year,
	t1.sale_month,
	t1.sale_amt,
	t1.wholesale_amt,
	t1.loss_amt,
	t1.taste_amt,
	t1.give_amt,
	t1.discount_amt,
	t1.sum_cost_amt,
	t1.customer_qty,
	t1.customer_price
from rst.rst_product_target_monitor_month t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_target_monitor_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--product_target_monitor_year
delete from rst.rst_product_target_monitor_year t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into rst.rst_product_target_monitor_year
(
	shop_code,
	shop_name,
	product_type,
	product_code,
	product_name,
	sale_year,
	sale_amt,
	wholesale_amt,
	loss_amt,
	taste_amt,
	give_amt,
	discount_amt,
	sum_cost_amt,
	customer_qty,
	customer_price
)
select 
	t2.new_shop_code shop_code,
	t2.new_shop_name shop_name,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.sale_year,
	t1.sale_amt,
	t1.wholesale_amt,
	t1.loss_amt,
	t1.taste_amt,
	t1.give_amt,
	t1.discount_amt,
	t1.sum_cost_amt,
	t1.customer_qty,
	t1.customer_price
from rst.rst_product_target_monitor_year t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from rst.rst_product_target_monitor_year t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_accept_shorts_day=========================================================
delete from analysis.analysis_accept_shorts_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_accept_shorts_day
(
	sale_date, shop_code, shop_name, shop_type, accept_qty, shorts_qty, shorts_rate
)
select 
	t1.sale_date, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.accept_qty, 
	t1.shorts_qty, 
	t1.shorts_rate
from analysis.analysis_accept_shorts_day t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_accept_shorts_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_accept_shorts_month
delete from analysis.analysis_accept_shorts_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_accept_shorts_month
(
	sale_year, sale_month, shop_code, shop_name, shop_type, accept_qty, shorts_qty, shorts_rate
)
select 
	t1.sale_year, 
	t1.sale_month, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.accept_qty, 
	t1.shorts_qty, 
	t1.shorts_rate
from analysis.analysis_accept_shorts_month t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_accept_shorts_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_accept_shorts_week
delete from analysis.analysis_accept_shorts_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_accept_shorts_week
(
	sale_year, sale_week, shop_code, shop_name, shop_type, accept_qty, shorts_qty, shorts_rate
)
select 
	t1.sale_year, 
	t1.sale_week, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.accept_qty, 
	t1.shorts_qty, 
	t1.shorts_rate
from analysis.analysis_accept_shorts_week t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_accept_shorts_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_check_error_day
delete from analysis.analysis_check_error_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_check_error_day
(
	sale_date, shop_code, shop_name, shop_type, error_qty, sale_qty, error_percent
)
select 
	t1.sale_date, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.error_qty, 
	t1.sale_qty, 
	t1.error_percent
from analysis.analysis_check_error_day t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_check_error_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_check_error_detail
delete from analysis.analysis_check_error_detail t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_check_error_detail
(
	sale_date, shop_code, shop_name, product_type, product_code, product_name, begin_qty, in_qty, allot_in_qty, allot_out_qty, sale_qty, loss_qty, end_qty, calculate_end_qty, error
)
select 
	t1.sale_date, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.product_type, 
	t1.product_code, 
	t1.product_name, 
	t1.begin_qty, 
	t1.in_qty, 
	t1.allot_in_qty, 
	t1.allot_out_qty, 
	t1.sale_qty, 
	t1.loss_qty, 
	t1.end_qty, 
	t1.calculate_end_qty, 
	t1.error
from analysis.analysis_check_error_detail t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_check_error_detail t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_check_error_week
delete from analysis.analysis_check_error_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_check_error_week
(
	sale_year, sale_week, shop_code, shop_name, shop_type, error_qty, error_percent
)
select 
	t1.sale_year, 
	t1.sale_week, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.error_qty, 
	t1.error_percent
from analysis.analysis_check_error_week t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_check_error_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_cost_day
delete from analysis.analysis_cost_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_cost_day
(
	sale_date, shop_code, shop_name, shop_type, sale_amt, loss_amt, taste_amt, give_amt, discount_amt, sum_cost_amt, loss_percent, sum_cost_percent
)
select 
	t1.sale_date, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.sale_amt, 
	t1.loss_amt, 
	t1.taste_amt, 
	t1.give_amt, 
	t1.discount_amt, 
	t1.sum_cost_amt, 
	t1.loss_percent, 
	t1.sum_cost_percent
from analysis.analysis_cost_day t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_cost_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_cost_detail
delete from analysis.analysis_cost_detail t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_cost_detail
(
	sale_date, shop_code, shop_name, shop_type, product_code, product_name, sale_amt, loss_amt, taste_amt, give_amt, discount_amt, sum_cost_amt, loss_percent, sum_cost_percent
)
select 
	t1.sale_date, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.product_code, 
	t1.product_name, 
	t1.sale_amt, 
	t1.loss_amt, 
	t1.taste_amt, 
	t1.give_amt, 
	t1.discount_amt, 
	t1.sum_cost_amt, 
	t1.loss_percent, 
	t1.sum_cost_percent
from analysis.analysis_cost_detail t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_cost_detail t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_cost_month
delete from analysis.analysis_cost_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_cost_month
(
	sale_year, sale_month, shop_code, shop_name, shop_type, sale_amt, loss_amt, taste_amt, give_amt, discount_amt, sum_cost_amt, loss_percent, sum_cost_percent
)
select 
	t1.sale_year, 
	t1.sale_month, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.sale_amt, 
	t1.loss_amt, 
	t1.taste_amt, 
	t1.give_amt, 
	t1.discount_amt, 
	t1.sum_cost_amt, 
	t1.loss_percent, 
	t1.sum_cost_percent
from analysis.analysis_cost_month t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_cost_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_cost_week
delete from analysis.analysis_cost_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_cost_week
(
	sale_year, sale_week, shop_code, shop_name, shop_type, sale_amt, loss_amt, taste_amt, give_amt, discount_amt, sum_cost_amt, loss_percent, sum_cost_percent
)
select 
	t1.sale_year, 
	t1.sale_week, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.sale_amt, 
	t1.loss_amt, 
	t1.taste_amt, 
	t1.give_amt, 
	t1.discount_amt, 
	t1.sum_cost_amt, 
	t1.loss_percent, 
	t1.sum_cost_percent
from analysis.analysis_cost_week t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_cost_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_main_index_day
delete from analysis.analysis_main_index_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_main_index_day
(
	sale_date, shop_code, shop_name, shop_type, sale_amt, wholesale_amt, customer_qty, customer_price, one_week, two_week, three_week, cost_sale_amt, loss_amt, sum_cost_amt, loss_percent, sum_cost_percent, shorts_rate, profit, profit_rate
)
select 
	t1.sale_date, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.sale_amt, 
	t1.wholesale_amt, 
	t1.customer_qty, 
	t1.customer_price, 
	t1.one_week, 
	t1.two_week, 
	t1.three_week, 
	t1.cost_sale_amt, 
	t1.loss_amt, 
	t1.sum_cost_amt, 
	t1.loss_percent, 
	t1.sum_cost_percent, 
	t1.shorts_rate, 
	t1.profit, 
	t1.profit_rate
from analysis.analysis_main_index_day t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_main_index_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_main_index_history
delete from analysis.analysis_main_index_history t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_main_index_history
(
	shop_code, shop_name, shop_type, sale_amt, customer_qty, sum_cost_percent, loss_percent, shorts_rate, profit_rate
)
select 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.sale_amt, 
	t1.customer_qty, 
	t1.sum_cost_percent, 
	t1.loss_percent, 
	t1.shorts_rate, 
	t1.profit_rate
from analysis.analysis_main_index_history t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_main_index_history t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_main_index_month
delete from analysis.analysis_main_index_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_main_index_month
(
	sale_year, sale_month, shop_code, shop_name, shop_type, sale_amt, wholesale_amt, customer_qty, customer_price, one_month, one_year, one_month_year, cost_sale_amt, loss_amt, sum_cost_amt, loss_percent, sum_cost_percent, shorts_rate, profit, profit_rate
)
select 
	t1.sale_year, 
	t1.sale_month, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.sale_amt, 
	t1.wholesale_amt, 
	t1.customer_qty, 
	t1.customer_price, 
	t1.one_month, 
	t1.one_year, 
	t1.one_month_year, 
	t1.cost_sale_amt, 
	t1.loss_amt, 
	t1.sum_cost_amt, 
	t1.loss_percent, 
	t1.sum_cost_percent, 
	t1.shorts_rate, 
	t1.profit, 
	t1.profit_rate
from analysis.analysis_main_index_month t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_main_index_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_main_index_week
delete from analysis.analysis_main_index_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_main_index_week
(
	sale_year, sale_week, shop_code, shop_name, shop_type, sale_amt, wholesale_amt, customer_qty, customer_price, one_week, two_week, three_week, cost_sale_amt, loss_amt, sum_cost_amt, loss_percent, sum_cost_percent, shorts_rate, profit, profit_rate
)
select 
	t1.sale_year, 
	t1.sale_week, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.sale_amt, 
	t1.wholesale_amt, 
	t1.customer_qty, 
	t1.customer_price, 
	t1.one_week, 
	t1.two_week, 
	t1.three_week, 
	t1.cost_sale_amt, 
	t1.loss_amt, 
	t1.sum_cost_amt, 
	t1.loss_percent, 
	t1.sum_cost_percent, 
	t1.shorts_rate, 
	t1.profit, 
	t1.profit_rate
from analysis.analysis_main_index_week t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_main_index_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_model_accept
delete from analysis.analysis_model_accept t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_model_accept
(
	arrive_date, shop_code, shop_name, shop_type, purchase_qty, accept_rate, broad_accept_rate
)
select 
	t1.arrive_date, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.purchase_qty, 
	t1.accept_rate, 
	t1.broad_accept_rate
from analysis.analysis_model_accept t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_model_accept t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_model_accept_type
delete from analysis.analysis_model_accept_type t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_model_accept_type
(
	arrive_date, shop_code, shop_name, shop_type, product_type, purchase_qty, accept_rate, broad_accept_rate
)
select 
	t1.arrive_date, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.product_type, 
	t1.purchase_qty, 
	t1.accept_rate, 
	t1.broad_accept_rate
from analysis.analysis_model_accept_type t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_model_accept_type t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_model_better_percent
delete from analysis.analysis_model_better_percent t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_model_better_percent
(
	arrive_date, shop_code, shop_name, shop_type, purchase_qty, accept_shorts_qty, accept_no_shorts_qty, more_shorts_qty, more_no_shorts_good_qty, more_no_shorts_bad_qty, less_shorts_qty, less_no_shorts_qty, model_better_percent
)
select 
	t1.arrive_date, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.purchase_qty, 
	t1.accept_shorts_qty, 
	t1.accept_no_shorts_qty, 
	t1.more_shorts_qty, 
	t1.more_no_shorts_good_qty,
	t1.more_no_shorts_bad_qty, 
	t1.less_shorts_qty, 
	t1.less_no_shorts_qty, 
	t1.model_better_percent
from analysis.analysis_model_better_percent t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_model_better_percent t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_shop_shorts_day
delete from analysis.analysis_shop_shorts_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_shop_shorts_day
(
	sale_date, shop_code, shop_name, shop_type, shorts_qty, shop_rate
)
select 
	t1.sale_date, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.shorts_qty, 
	t1.shop_rate
from analysis.analysis_shop_shorts_day t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_shop_shorts_day t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_shop_shorts_month
delete from analysis.analysis_shop_shorts_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_shop_shorts_month
(
	sale_year, sale_month, shop_code, shop_name, shop_type, shorts_qty, shop_rate
)
select 
	t1.sale_year, 
	t1.sale_month,
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.shorts_qty, 
	t1.shop_rate
from analysis.analysis_shop_shorts_month t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_shop_shorts_month t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_shop_shorts_week
delete from analysis.analysis_shop_shorts_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_shop_shorts_week
(
	sale_year, sale_week, shop_code, shop_name, shop_type, shorts_qty, shop_rate
)
select 
	t1.sale_year, 
	t1.sale_week,
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.shorts_qty, 
	t1.shop_rate
from analysis.analysis_shop_shorts_week t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_shop_shorts_week t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;


--analysis.analysis_shorts_detail
delete from analysis.analysis_shorts_detail t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.new_shop_code
	and t2.change_date=$1
;

insert into analysis.analysis_shorts_detail
(
	sale_date, shop_code, shop_name, shop_type, product_code, product_name, optimal_purchase, final_purchase, adjust_qty, shorts_time, shop_rate, product_rate
)
select 
	t1.sale_date, 
	t2.new_shop_code shop_code, 
	t2.new_shop_name shop_name, 
	t1.shop_type, 
	t1.product_code, 
	t1.product_name, 
	t1.optimal_purchase, 
	t1.final_purchase, 
	t1.adjust_qty, 
	t1.shorts_time, 
	t1.shop_rate, 
	t1.product_rate
from analysis.analysis_shorts_detail t1
inner join rst.rst_shop_code_change t2 
	on t1.shop_code=t2.old_shop_code
where t2.change_date=$1
;

delete from analysis.analysis_shorts_detail t1 
using rst.rst_shop_code_change t2 
where t1.shop_code=t2.old_shop_code
	and t2.change_date=$1
;



	RETURN 1;
END

$function$
;

```