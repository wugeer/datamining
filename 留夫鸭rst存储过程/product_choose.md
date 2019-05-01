# product_choose
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_choose()
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

truncate table rst.rst_product_choose;
insert into rst.rst_product_choose
(
	product_code,
	product_name,
	franchiser_class,
	product_type,
	measure_unit,
	flavor,
	warranty,
	shelf_life,
	material,
	is_valid,
	is_order,
	min_qty
) 
select 
	product_code,
	product_name,
	franchiser_class,
	product_type,
	measure_unit,
	flavor,
	warranty,
	shelf_life,
	material,
	is_valid,
	is_order,
	min_qty
from edw.dim_product 
where product_code like '05%' 
	and product_type <>'辅料类' 
	and product_type <>'物料类'
	and product_type is not null
;

	RETURN 1;
END

$function$
;

```