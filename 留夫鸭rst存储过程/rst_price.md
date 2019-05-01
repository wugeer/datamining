# rst_price
```
CREATE OR REPLACE FUNCTION rst.p_rst_price()
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

truncate table rst.rst_price;
insert into rst.rst_price
(
	shop_code,
	product_code,
	price_sale,
	price_wholesale
) 
select 
	shop_code,
	product_code,
	price_sale,
	price_wholesale
from edw.dim_price
;

	RETURN 1;
END

$function$
;

```