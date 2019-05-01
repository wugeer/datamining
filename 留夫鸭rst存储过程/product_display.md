# product_display
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_display()
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--取所有门店的所有商品的陈列量
drop table if exists tmp1;
create temporary table tmp1 as 
select
	t1.shop_code,
	t2.product_code,
	0 min_display_qty
from edw.dim_shop t1 
cross join edw.dim_product t2 
;

--更新陈列量
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	t1.shop_code,
	t1.product_code,
	coalesce(t2.min_display_qty,t1.min_display_qty) min_display_qty
from tmp1 t1 
left join rst.rst_product_display t2 
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
;

truncate table rst.rst_product_display;
insert into rst.rst_product_display 
(
	shop_code,
	product_code,
	min_display_qty
)
select 
	shop_code,
	product_code,
	min_display_qty
from tmp2
;

drop table if exists tmp1;
drop table if exists tmp2;

	RETURN 1;
END

$function$
;

```