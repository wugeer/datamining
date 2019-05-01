# product_short_ana_update
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_short_analysis_update(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--更新当日最终要货量
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
	t1.shorts_time,
	t1.check_status,
	t1.computer_end_qty,
	t1.d0_end_qty,
	t1.d1_in_qty,
	t1.d2_in_qty,
	t1.d2_end_qty_predict,
	t1.optimal_purchase,
	t2.final_purchase,
	t1.d3_sale_predict,
	t1.actual_sale_qty,
	t1.loss_qty,
	t1.taste_qty,
	t1.give_qty,
	t1.allot_qty,
	t1.d3_sale_actual
from rst.rst_product_short_analysis t1 
left join rst.rst_product_bill_shop t2
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
	and t1.bill_date=t2.bill_date
where t1.bill_date=$1
;

	
--插入当天数据
delete from rst.rst_product_short_analysis where bill_date=$1;
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
	t1.shop_code,
	t1.shop_name,
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
from tmp1 t1 
where t1.bill_date=$1
;

drop table if exists tmp1;

	RETURN 1;
END

$function$
;

```