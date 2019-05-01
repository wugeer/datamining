# product_sale_week
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_sale_week(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--每家门店每种商品的本周销量
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t3.shop_code,
	t3.product_code,
	t2.sale_year,
	t2.sale_week,
	sum(coalesce(t3.qty,0)) qty
from rst.rst_week t1
left join rst.rst_week t2
	on t1.sale_year=t2.sale_year
	and t1.sale_week=t2.sale_week
inner join rst.rst_product_sale_day t3
	on t2.sale_date=t3.sale_date
where t1.sale_date=$1
group by 
	t3.shop_code,
	t3.product_code,
	t2.sale_year,
	t2.sale_week
having sum(coalesce(t3.qty,0))<>0
;

--每家门店每种商品的上1周销量
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	t3.shop_code,
	t3.product_code,
	sum(coalesce(t3.qty,0)) one_ago_qty
from rst.rst_week t1
left join rst.rst_week t2
	on t1.sale_year=t2.sale_year
	and t1.sale_week=t2.sale_week
inner join rst.rst_product_sale_day t3
	on t2.sale_date=t3.sale_date
where t1.sale_date=($1-'1 week'::interval)::date
group by 
	t3.shop_code,
	t3.product_code
having sum(coalesce(t3.qty,0))<>0
;

--每家门店每种商品的上2周销量
drop table if exists tmp3;
create temporary table tmp3 as 
select 
	t3.shop_code,
	t3.product_code,
	sum(coalesce(t3.qty,0)) two_ago_qty
from rst.rst_week t1
left join rst.rst_week t2
	on t1.sale_year=t2.sale_year
	and t1.sale_week=t2.sale_week
inner join rst.rst_product_sale_day t3
	on t2.sale_date=t3.sale_date
where t1.sale_date=($1-'2 week'::interval)::date
group by 
	t3.shop_code,
	t3.product_code
having sum(coalesce(t3.qty,0))<>0
;

--每家门店每种商品的上3周销量
drop table if exists tmp4;
create temporary table tmp4 as 
select 
	t3.shop_code,
	t3.product_code,
	sum(coalesce(t3.qty,0)) three_ago_qty
from rst.rst_week t1
left join rst.rst_week t2
	on t1.sale_year=t2.sale_year
	and t1.sale_week=t2.sale_week
inner join rst.rst_product_sale_day t3
	on t2.sale_date=t3.sale_date
where t1.sale_date=($1-'3 week'::interval)::date
group by 
	t3.shop_code,
	t3.product_code
having sum(coalesce(t3.qty,0))<>0
;

--每家门店每种商品的上4周销量
drop table if exists tmp5;
create temporary table tmp5 as 
select 
	t3.shop_code,
	t3.product_code,
	sum(coalesce(t3.qty,0)) four_ago_qty
from rst.rst_week t1
left join rst.rst_week t2
	on t1.sale_year=t2.sale_year
	and t1.sale_week=t2.sale_week
inner join rst.rst_product_sale_day t3
	on t2.sale_date=t3.sale_date
where t1.sale_date=($1-'4 week'::interval)::date
group by 
	t3.shop_code,
	t3.product_code
having sum(coalesce(t3.qty,0))<>0
;

--每家门店每种商品的上5周销量
drop table if exists tmp6;
create temporary table tmp6 as 
select 
	t3.shop_code,
	t3.product_code,
	sum(coalesce(t3.qty,0)) five_ago_qty
from rst.rst_week t1
left join rst.rst_week t2
	on t1.sale_year=t2.sale_year
	and t1.sale_week=t2.sale_week
inner join rst.rst_product_sale_day t3
	on t2.sale_date=t3.sale_date
where t1.sale_date=($1-'5 week'::interval)::date
group by 
	t3.shop_code,
	t3.product_code
having sum(coalesce(t3.qty,0))<>0
;

--每家门店每种商品的上6周销量
drop table if exists tmp7;
create temporary table tmp7 as 
select 
	t3.shop_code,
	t3.product_code,
	sum(coalesce(t3.qty,0)) six_ago_qty
from rst.rst_week t1
left join rst.rst_week t2
	on t1.sale_year=t2.sale_year
	and t1.sale_week=t2.sale_week
inner join rst.rst_product_sale_day t3
	on t2.sale_date=t3.sale_date
where t1.sale_date=($1-'6 week'::interval)::date
group by 
	t3.shop_code,
	t3.product_code
having sum(coalesce(t3.qty,0))<>0
;

--插入数据
delete from rst.rst_product_sale_week t1
using rst.rst_week t2
where t1.sale_year=t2.sale_year
	and t1.sale_week=t2.sale_week
	and t2.sale_date=$1
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
	t1.shop_code,
	t8.shop_name,
	t9.product_type,
	t1.product_code,
	t9.product_name,
	t1.sale_year,
	t1.sale_week,
	t1.qty,
	t2.one_ago_qty,
	t3.two_ago_qty,
	t4.three_ago_qty,
	t5.four_ago_qty,
	t6.five_ago_qty,
	t7.six_ago_qty,
	t9.measure_unit
from tmp1 t1
left join tmp2 t2
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
left join tmp3 t3
	on t1.shop_code=t3.shop_code
	and t1.product_code=t3.product_code
left join tmp4 t4
	on t1.shop_code=t4.shop_code
	and t1.product_code=t4.product_code
left join tmp5 t5
	on t1.shop_code=t5.shop_code
	and t1.product_code=t5.product_code
left join tmp6 t6
	on t1.shop_code=t6.shop_code
	and t1.product_code=t6.product_code
left join tmp7 t7
	on t1.shop_code=t7.shop_code
	and t1.product_code=t7.product_code 
left join rst.rst_shop t8
	on t1.shop_code=t8.shop_code
left join rst.rst_product t9 
	on t1.product_code=t9.product_code
;

drop table if exists tmp1;
drop table if exists tmp2;
drop table if exists tmp3;
drop table if exists tmp4;
drop table if exists tmp5;
drop table if exists tmp6;
drop table if exists tmp7;

	RETURN 1;
END

$function$
;

```