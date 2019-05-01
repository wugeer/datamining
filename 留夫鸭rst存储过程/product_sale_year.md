# product_sale_year
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_sale_year(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--每家门店每种商品的本年销量
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	shop_code,
	product_code,
	extract(year from sale_date) sale_year,
	sum(coalesce(qty,0)) qty
from rst.rst_product_sale_day
where extract(year from sale_date)=extract(year from $1)
group by 
	shop_code,
	product_code,
	extract(year from sale_date)
having sum(coalesce(qty,0))<>0
;

--每家门店每种商品的去年销量
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	shop_code,
	product_code,
	extract(year from sale_date+'1 year'::interval) sale_year,
	sum(coalesce(qty,0)) com_qty
from rst.rst_product_sale_day
where extract(year from sale_date)=extract(year from $1-'1 year'::interval)
group by 
	shop_code,
	product_code,
	extract(year from sale_date+'1 year'::interval)
having sum(coalesce(qty,0))<>0
;

--插入数据
delete from rst.rst_product_sale_year
where sale_year=extract(year from $1)
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
	t1.shop_code,
	t3.shop_name,
	t4.product_type,
	t1.product_code,
	t4.product_name,
	t1.sale_year,
	t1.qty,
	t2.com_qty,
	t4.measure_unit
from tmp1 t1
left join tmp2 t2
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
left join rst.rst_shop t3 
	on t1.shop_code=t3.shop_code
left join rst.rst_product t4 
	on t1.product_code=t4.product_code
;

drop table if exists tmp1;
drop table if exists tmp2;

	RETURN 1;
END

$function$
;

```