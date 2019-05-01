# rst_product_bill_shop_update
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_bill_shop_update(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--补充维度数据，且替换create_time，剔除下架商品，过滤非智能要货门店
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t1.shop_code,
	t1.shop_name,
	t1.bill_date,
	t1.arrive_date,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t2.measure_unit,
	t2.min_qty,
	t3.min_display_qty,
	t5.price_sale,
	t5.price_wholesale,
	coalesce(t1.d0_end_qty,t8.qty) d0_end_qty,
	t1.d1_in_qty,
	t1.d2_in_qty,
	t1.d2_end_qty_predict,
	t1.optimal_purchase,
	case when t1.shop_purchase is null then 0 
		else t1.final_purchase
	end final_purchase,
	t1.d3_sale_predict,
	coalesce(t6.create_time,now()) create_time,
	null update_time,
	t1.feedback_reason,
	'0' del_flag,
	'0' off_flag,
	coalesce(t1.shop_purchase,0) shop_purchase,
	t1.operations_purchase,
	t7.district_manager_code,
	t7.district_manager_name,
	t7.district_director_code,
	t7.district_director_name,
	t1.supply_purchase
from ods.ods_product_bill_shop t1
left join rst.rst_product t2 
	on t1.product_code=t2.product_code
left join rst.rst_product_display t3 
	on t1.shop_code=t3.shop_code 
	and t1.product_code=t3.product_code
left join rst.rst_price t5 
	on t1.shop_code=t5.shop_code 
	and t1.product_code=t5.product_code
left join rst.rst_product_bill_shop t6 
	on t1.shop_code=t6.shop_code 
	and t1.product_code=t6.product_code
	and t1.bill_date=t6.bill_date
left join rst.rst_shop t7 
	on t1.shop_code=t7.shop_code
left join rst.rst_stock t8
	on t1.shop_code=t8.shop_code
	and t1.product_code=t8.product_code
	and t1.bill_date=(t8.check_date+'1 day'::interval)::date
inner join rst.rst_shop_test t9
	on t1.shop_code=t9.shop_code
where t1.bill_date=$1
	and t1.product_type is not null
	and t1.product_type <>'物料类'
	and not exists(
		select 1 from rst.rst_off_product t10 
			where t1.shop_code=t10.shop_code
				and t1.product_code=t10.product_code
				and t10.create_time::date=$1
		)
;
	

delete from rst.rst_product_bill_shop where bill_date=$1;
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
from tmp1
;

drop table if exists tmp1;

	RETURN 1;
END

$function$
;

```