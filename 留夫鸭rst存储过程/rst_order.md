# rst_order
```
CREATE OR REPLACE FUNCTION rst.p_rst_order(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

delete from rst.rst_order where order_date=$1;
insert into rst.rst_order 
(
	doc_no,
	shop_code,
	product_code,
	qty,
	amt,
	order_date,
	order_time,
	order_type,
	remark
) 
select 
	doc_no,
	shop_code,
	product_code,
	qty,
	amt,
	order_date,
	order_time,
	order_type,
	remark
from edw.fct_order
where order_date=$1
;

	RETURN 1;
END

$function$
;

```