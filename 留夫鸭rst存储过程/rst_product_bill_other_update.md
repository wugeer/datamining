# rst_product_bill_other_update
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_bill_other_update(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--取create_time，过滤非智能要货门店
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t1.goods_type,
	t1.shop_code,
	t1.shop_name,
	t2.shop_type,
	t1.bill_date,
	t1.arr_date,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t3.measure_unit,
	t3.min_qty,
	t6.qty night_qty,
	t5.price_sale,
	t5.price_wholesale,
	t1.shop_qty,
	t1.area_qty,
	t1.fin_qty,
	coalesce(t7.create_time,now()) create_time
from ods.ods_product_bill_other t1
left join rst.rst_shop t2 
	on t1.shop_code=t2.shop_code
left join rst.rst_product t3 
	on t1.product_code=t3.product_code
left join rst.rst_price t5
	on t1.shop_code=t5.shop_code
	and t1.product_code=t5.product_code
left join rst.rst_stock t6 
	on t1.shop_code=t6.shop_code
	and t1.product_code=t6.product_code
	and t1.bill_date=(t6.check_date+'1 day'::interval)::date
left join rst.rst_product_bill_other t7 
	on t1.goods_type=t7.goods_type
	and t1.shop_code=t7.shop_code
	and t1.product_code=t7.product_code
	and t1.bill_date=t7.bill_date
inner join rst.rst_shop_test t8
	on t1.shop_code=t8.shop_code
where t1.bill_date=$1
	and (t1.goods_type='物料要货' or t1.goods_type='团购要货')
;


delete from rst.rst_product_bill_other 
where bill_date=$1 
	and (goods_type='物料要货' or goods_type='团购要货')
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
from tmp1 
;

drop table if exists tmp1;

	RETURN 1;
END

$function$
;

```