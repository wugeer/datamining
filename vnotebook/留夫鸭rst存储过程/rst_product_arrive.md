# rst_product_arrive
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_arrive(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

delete from rst.rst_product_arrive where arrive_date=$1;
insert into rst.rst_product_arrive
(
	shop_code,
	product_code,
	arrive_date,
	arrive_qty,
	supply_qty,
	final_qty
)
select 
	shop_code,
	product_code,
	arrive_date,
	final_purchase arrive_qty,
	supply_purchase supply_qty,
	coalesce(final_purchase,0)+coalesce(supply_purchase,0) final_qty
from rst.rst_product_bill_shop
where arrive_date=$1
;

	RETURN 1;
END

$function$
;

```