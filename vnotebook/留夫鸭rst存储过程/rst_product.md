# rst_product
```
CREATE OR REPLACE FUNCTION rst.p_rst_product()
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

truncate table rst.rst_product;
insert into rst.rst_product 
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
	min_qty,
	prohibit_order
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
	min_qty,
	prohibit_order
from edw.dim_product
;

	RETURN 1;
END

$function$
;

```