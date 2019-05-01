# product_sale_date
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_sale_day(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN


--每家门店每种商品的当日销量
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	shop_code,
	product_code,
	sale_date,
	sum(qty) qty
from rst.rst_pos
where sale_date=$1
group by 
	shop_code,
	product_code,
	sale_date
having sum(qty)<>0
;

--每家门店每种商品的上1日销量
drop table if exists tmp3;
create temporary table tmp3 as 
select 
	shop_code,
	product_code,
	(sale_date+'1 day'::interval)::date sale_date,
	sum(qty) one_ago_qty
from rst.rst_pos
where sale_date=($1-'1 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'1 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上2日销量
drop table if exists tmp4;
create temporary table tmp4 as 
select 
	shop_code,
	product_code,
	(sale_date+'2 day'::interval)::date sale_date,
	sum(qty) two_ago_qty
from rst.rst_pos
where sale_date=($1-'2 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'2 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上3日销量
drop table if exists tmp5;
create temporary table tmp5 as 
select 
	shop_code,
	product_code,
	(sale_date+'3 day'::interval)::date sale_date,
	sum(qty) three_ago_qty
from rst.rst_pos
where sale_date=($1-'3 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'3 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上4日销量
drop table if exists tmp6;
create temporary table tmp6 as 
select 
	shop_code,
	product_code,
	(sale_date+'4 day'::interval)::date sale_date,
	sum(qty) four_ago_qty
from rst.rst_pos
where sale_date=($1-'4 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'4 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上5日销量
drop table if exists tmp7;
create temporary table tmp7 as 
select 
	shop_code,
	product_code,
	(sale_date+'5 day'::interval)::date sale_date,
	sum(qty) five_ago_qty
from rst.rst_pos
where sale_date=($1-'5 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'5 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上6日销量
drop table if exists tmp8;
create temporary table tmp8 as 
select 
	shop_code,
	product_code,
	(sale_date+'6 day'::interval)::date sale_date,
	sum(qty) six_ago_qty
from rst.rst_pos
where sale_date=($1-'6 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'6 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上7日销量
drop table if exists tmp9;
create temporary table tmp9 as 
select 
	shop_code,
	product_code,
	(sale_date+'7 day'::interval)::date sale_date,
	sum(qty) com_qty
from rst.rst_pos
where sale_date=($1-'7 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'7 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上8日销量
drop table if exists tmp10;
create temporary table tmp10 as 
select 
	shop_code,
	product_code,
	(sale_date+'8 day'::interval)::date sale_date,
	sum(qty) one_ago_com_qty
from rst.rst_pos
where sale_date=($1-'8 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'8 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上9日销量
drop table if exists tmp11;
create temporary table tmp11 as 
select 
	shop_code,
	product_code,
	(sale_date+'9 day'::interval)::date sale_date,
	sum(qty) two_ago_com_qty
from rst.rst_pos
where sale_date=($1-'9 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'9 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上10日销量
drop table if exists tmp12;
create temporary table tmp12 as 
select 
	shop_code,
	product_code,
	(sale_date+'10 day'::interval)::date sale_date,
	sum(qty) three_ago_com_qty
from rst.rst_pos
where sale_date=($1-'10 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'10 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上11日销量
drop table if exists tmp13;
create temporary table tmp13 as 
select 
	shop_code,
	product_code,
	(sale_date+'11 day'::interval)::date sale_date,
	sum(qty) four_ago_com_qty
from rst.rst_pos
where sale_date=($1-'11 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'11 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上12日销量
drop table if exists tmp14;
create temporary table tmp14 as 
select 
	shop_code,
	product_code,
	(sale_date+'12 day'::interval)::date sale_date,
	sum(qty) five_ago_com_qty
from rst.rst_pos
where sale_date=($1-'12 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'12 day'::interval)::date
having sum(qty)<>0
;

--每家门店每种商品的上13日销量
drop table if exists tmp15;
create temporary table tmp15 as 
select 
	shop_code,
	product_code,
	(sale_date+'13 day'::interval)::date sale_date,
	sum(qty) six_ago_com_qty
from rst.rst_pos
where sale_date=($1-'13 day'::interval)::date
group by 
	shop_code,
	product_code,
	(sale_date+'13 day'::interval)::date
having sum(qty)<>0
;

--合并
delete from rst.rst_product_sale_day where sale_date=$1;
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
	t1.shop_code,
	t16.shop_name,
	t17.product_type,
	t1.product_code,
	t17.product_name,
	t1.sale_date::date,
	t1.qty,
	t3.one_ago_qty,
	t4.two_ago_qty,
	t5.three_ago_qty,
	t6.four_ago_qty,
	t7.five_ago_qty,
	t8.six_ago_qty,
	t9.com_qty,
	t10.one_ago_com_qty,
	t11.two_ago_com_qty,
	t12.three_ago_com_qty,
	t13.four_ago_com_qty,
	t14.five_ago_com_qty,
	t15.six_ago_com_qty,
	t17.measure_unit
from tmp2 t1
left join tmp3 t3 
	on t1.shop_code=t3.shop_code 
	and t1.product_code=t3.product_code 
	and t1.sale_date=t3.sale_date
left join tmp4 t4 
	on t1.shop_code=t4.shop_code
	and t1.product_code=t4.product_code
	and t1.sale_date=t4.sale_date
left join tmp5 t5 
	on t1.shop_code=t5.shop_code
	and t1.product_code=t5.product_code
	and t1.sale_date=t5.sale_date
left join tmp6 t6 
	on t1.shop_code=t6.shop_code
	and t1.product_code=t6.product_code
	and t1.sale_date=t6.sale_date
left join tmp7 t7 
	on t1.shop_code=t7.shop_code
	and t1.product_code=t7.product_code
	and t1.sale_date=t7.sale_date
left join tmp8 t8 
	on t1.shop_code=t8.shop_code
	and t1.product_code=t8.product_code
	and t1.sale_date=t8.sale_date
left join tmp9 t9 
	on t1.shop_code=t9.shop_code
	and t1.product_code=t9.product_code
	and t1.sale_date=t9.sale_date
left join tmp10 t10 
	on t1.shop_code=t10.shop_code
	and t1.product_code=t10.product_code
	and t1.sale_date=t10.sale_date
left join tmp11 t11 
	on t1.shop_code=t11.shop_code
	and t1.product_code=t11.product_code
	and t1.sale_date=t11.sale_date
left join tmp12 t12 
	on t1.shop_code=t12.shop_code
	and t1.product_code=t12.product_code
	and t1.sale_date=t12.sale_date
left join tmp13 t13 
	on t1.shop_code=t13.shop_code
	and t1.product_code=t13.product_code
	and t1.sale_date=t13.sale_date
left join tmp14 t14 
	on t1.shop_code=t14.shop_code
	and t1.product_code=t14.product_code
	and t1.sale_date=t14.sale_date
left join tmp15 t15 
	on t1.shop_code=t15.shop_code
	and t1.product_code=t15.product_code
	and t1.sale_date=t15.sale_date
left join edw.dim_shop t16 
	on t1.shop_code=t16.shop_code
left join edw.dim_product t17 
	on t1.product_code=t17.product_code
where t17.product_type is not null
;

drop table if exists tmp2;
drop table if exists tmp3;
drop table if exists tmp4;
drop table if exists tmp5;
drop table if exists tmp6;
drop table if exists tmp7;
drop table if exists tmp8;
drop table if exists tmp9;
drop table if exists tmp10;
drop table if exists tmp11;
drop table if exists tmp12;
drop table if exists tmp13;
drop table if exists tmp14;
drop table if exists tmp15;

	RETURN 1;
END

$function$
;

```