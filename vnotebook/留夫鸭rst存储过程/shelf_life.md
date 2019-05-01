# shelf_life
```
CREATE OR REPLACE FUNCTION rst.p_rst_shelf_life()
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--取所有门店的所有商品的默认货架期
drop table if exists tmp1;
create temporary table tmp1 as 
select
	t1.shop_code,
	t2.product_code,
	t2.warranty,
	case 
		when t2.warranty='1 day'::interval then '1 day'::interval 
		when t2.warranty='2 day'::interval then '1 day'::interval 
		when t2.warranty='3 day'::interval then '2 day'::interval 
		when t2.warranty='4 day'::interval then '3 day'::interval 
		when t2.warranty='5 day'::interval then '4 day'::interval 
		when t2.warranty='6 day'::interval then '5 day'::interval 
		when t2.warranty='7 day'::interval then '6 day'::interval 
		else t2.warranty
	end shelf_life,
	case 
		when t2.warranty='1 day'::interval then '1 day'::interval 
		when t2.warranty='2 day'::interval then '1 day'::interval 
		when t2.warranty='3 day'::interval then '1 day'::interval 
		when t2.warranty='4 day'::interval then '1 day'::interval 
		when t2.warranty='5 day'::interval then '2 day'::interval 
		when t2.warranty='6 day'::interval then '3 day'::interval 
		when t2.warranty='7 day'::interval then '4 day'::interval 
		else t2.warranty
	end best_life
from edw.dim_shop t1 
cross join edw.dim_product t2 
where t2.warranty is not null
;

/*
--更新货架期
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	t1.shop_code,
	t1.product_code,
	t1.warranty,
	coalesce(t2.shelf_life::interval,t1.shelf_life) shelf_life
from tmp1 t1 
left join ods.ods_shelf_life t2 
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
;
*/

--调整单品最佳品尝期
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	shop_code,
	product_code,
	warranty,
	shelf_life,
	case 
		when product_code='05030005' then '1 day'::interval 
		when product_code='05324001' then '1 day'::interval 
		when product_code='05050076' then '1 day'::interval 
		else best_life
	end best_life
from tmp1
;


truncate table rst.rst_shelf_life;
insert into rst.rst_shelf_life 
(
	shop_code,
	product_code,
	warranty,
	shelf_life,
	best_life
)
select 
	shop_code,
	product_code,
	warranty,
	shelf_life,
	best_life
from tmp2
;

drop table if exists tmp1;
drop table if exists tmp2;

	RETURN 1;
END

$function$
;

```