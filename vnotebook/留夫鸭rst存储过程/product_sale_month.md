# product_sale_month
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_sale_month(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--每家门店每种商品的当月销量
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	shop_code,
	product_code,
	extract(year from sale_date) sale_year,
	extract(month from sale_date) sale_month,
	sum(coalesce(qty,0)) qty
from rst.rst_product_sale_day
where extract(year from sale_date)=extract(year from $1)
	and extract(month from sale_date)=extract(month from $1)
group by 
	shop_code,
	product_code,
	extract(year from sale_date),
	extract(month from sale_date)
having sum(coalesce(qty,0))<>0
;

--每家门店每种商品的上1月销量
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	shop_code,
	product_code,
	extract(year from sale_date+'1 month'::interval) sale_year,
	extract(month from sale_date+'1 month'::interval) sale_month,
	sum(coalesce(qty,0)) one_ago_qty
from rst.rst_product_sale_day
where extract(year from sale_date)=extract(year from $1-'1 month'::interval)
	and extract(month from sale_date)=extract(month from $1-'1 month'::interval)
group by 
	shop_code,
	product_code,
	extract(year from sale_date+'1 month'::interval),
	extract(month from sale_date+'1 month'::interval)
having sum(coalesce(qty,0))<>0
;

--每家门店每种商品的上2月销量
drop table if exists tmp3;
create temporary table tmp3 as 
select 
	shop_code,
	product_code,
	extract(year from sale_date+'2 month'::interval) sale_year,
	extract(month from sale_date+'2 month'::interval) sale_month,
	sum(coalesce(qty,0)) two_ago_qty
from rst.rst_product_sale_day
where extract(year from sale_date)=extract(year from $1-'2 month'::interval)
	and extract(month from sale_date)=extract(month from $1-'2 month'::interval)
group by 
	shop_code,
	product_code,
	extract(year from sale_date+'2 month'::interval),
	extract(month from sale_date+'2 month'::interval)
having sum(coalesce(qty,0))<>0
;

--每家门店每种商品的去年当月销量
drop table if exists tmp4;
create temporary table tmp4 as 
select 
	shop_code,
	product_code,
	extract(year from sale_date+'1 year'::interval) sale_year,
	extract(month from sale_date+'1 year'::interval) sale_month,
	sum(coalesce(qty,0)) com_qty
from rst.rst_product_sale_day
where extract(year from sale_date)=extract(year from $1-'1 year'::interval)
	and extract(month from sale_date)=extract(month from $1-'1 year'::interval)
group by 
	shop_code,
	product_code,
	extract(year from sale_date+'1 year'::interval),
	extract(month from sale_date+'1 year'::interval)
having sum(coalesce(qty,0))<>0
;

--每家门店每种商品的去年上1月销量
drop table if exists tmp5;
create temporary table tmp5 as 
select 
	shop_code,
	product_code,
	extract(year from sale_date+'1 month'::interval+'1 year'::interval) sale_year,
	extract(month from sale_date+'1 month'::interval+'1 year'::interval) sale_month,
	sum(coalesce(qty,0)) one_ago_com_qty
from rst.rst_product_sale_day
where extract(year from sale_date)=extract(year from $1-'1 month'::interval-'1 year'::interval)
	and extract(month from sale_date)=extract(month from $1-'1 month'::interval-'1 year'::interval)
group by 
	shop_code,
	product_code,
	extract(year from sale_date+'1 month'::interval+'1 year'::interval),
	extract(month from sale_date+'1 month'::interval+'1 year'::interval)
having sum(coalesce(qty,0))<>0
;

--每家门店每种商品的去年上2月销量
drop table if exists tmp6;
create temporary table tmp6 as 
select 
	shop_code,
	product_code,
	extract(year from sale_date+'2 month'::interval+'1 year'::interval) sale_year,
	extract(month from sale_date+'2 month'::interval+'1 year'::interval) sale_month,
	sum(coalesce(qty,0)) two_ago_com_qty
from rst.rst_product_sale_day
where extract(year from sale_date)=extract(year from $1-'2 month'::interval-'1 year'::interval)
	and extract(month from sale_date)=extract(month from $1-'2 month'::interval-'1 year'::interval)
group by 
	shop_code,
	product_code,
	extract(year from sale_date+'2 month'::interval+'1 year'::interval),
	extract(month from sale_date+'2 month'::interval+'1 year'::interval)
having sum(coalesce(qty,0))<>0
;

--插入数据
delete from rst.rst_product_sale_month 
where sale_year=extract(year from $1)
	and sale_month=extract(month from $1)
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
	t1.shop_code,
	t7.shop_name,
	t8.product_type,
	t1.product_code,
	t8.product_name,
	t1.sale_year,
	t1.sale_month,
	t1.qty,
	t2.one_ago_qty,
	t3.two_ago_qty,
	t4.com_qty,
	t5.one_ago_com_qty,
	t6.two_ago_com_qty,
	t8.measure_unit
from tmp1 t1
left join tmp2 t2
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
	and t1.sale_year=t2.sale_year
	and t1.sale_month=t2.sale_month
left join tmp3 t3
	on t1.shop_code=t3.shop_code
	and t1.product_code=t3.product_code
	and t1.sale_year=t3.sale_year
	and t1.sale_month=t3.sale_month
left join tmp4 t4
	on t1.shop_code=t4.shop_code
	and t1.product_code=t4.product_code
	and t1.sale_year=t4.sale_year
	and t1.sale_month=t4.sale_month
left join tmp5 t5
	on t1.shop_code=t5.shop_code
	and t1.product_code=t5.product_code
	and t1.sale_year=t5.sale_year
	and t1.sale_month=t5.sale_month
left join tmp6 t6
	on t1.shop_code=t6.shop_code
	and t1.product_code=t6.product_code
	and t1.sale_year=t6.sale_year
	and t1.sale_month=t6.sale_month
left join rst.rst_shop t7
	on t1.shop_code=t7.shop_code
left join rst.rst_product t8
	on t1.product_code=t8.product_code
;

drop table if exists tmp1;
drop table if exists tmp2;
drop table if exists tmp3;
drop table if exists tmp4;
drop table if exists tmp5;
drop table if exists tmp6;

	RETURN 1;
END

$function$
;

```