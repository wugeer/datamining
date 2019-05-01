# rst_stock
```
CREATE OR REPLACE FUNCTION rst.p_rst_stock(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

delete from rst.rst_stock where check_date=$1;
insert into rst.rst_stock 
(
	shop_code,
	product_code,
	qty,
	check_date
) 
select 
	shop_code,
	product_code,
	qty,
	check_date
from edw.fct_stock 
where check_date=$1
;


	RETURN 1;
END

$function$
;

```