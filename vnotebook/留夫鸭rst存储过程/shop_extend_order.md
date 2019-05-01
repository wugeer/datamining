# shop_extend_order
```
CREATE OR REPLACE FUNCTION rst.p_rst_shop_extend_order(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--有销售记录的门店
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	shop_code,
	sale_date
from rst.rst_pos 
where sale_date>='20180715'::date
	and sale_date<=$1
group by 
	shop_code,
	sale_date
;

--无库存盘点的门店
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	shop_code,
	check_date
from rst.rst_stock 
where check_date>='20180715'::date
	and check_date<=$1
group by 
	shop_code,
	check_date
;

--有销售记录，无库存盘点门店的天数
drop table if exists tmp3;
create temporary table tmp3 as 
select 
	t1.shop_code,
	count(1) date_qty
from tmp1 t1
where not exists(
		select 1 from tmp2 t2 
		where t1.shop_code=t2.shop_code 
		and t1.sale_date=t2.check_date
		)
group by 
	t1.shop_code
;

--昨日有销售记录的门店
drop table if exists tmp4;
create temporary table tmp4 as 
select 
	shop_code
from rst.rst_pos 
where sale_date=$1
group by 
	shop_code
;

truncate table rst.rst_shop_extend_order;
insert into rst.rst_shop_extend_order 
(
	shop_code,
	shop_name,
	shop_type,
	shop_class,
	distribute_route,
	route_order,
	date_qty
) 
select 
	t1.shop_code,
	t1.shop_name,
	t1.shop_type,
	t1.shop_class,
	t1.distribute_route,
	t2.route_order,
	t3.date_qty
from rst.rst_shop t1
inner join tmp4 t4 
	on t1.shop_code=t4.shop_code
left join rst.rst_route_order t2 
	on t1.distribute_route=t2.distribute_route
left join tmp3 t3 
	on t1.shop_code=t3.shop_code
;

drop table if exists tmp1;
drop table if exists tmp2;
drop table if exists tmp3;

	RETURN 1;
END

$function$
;

```