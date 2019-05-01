# rst_pos
```
CREATE OR REPLACE FUNCTION rst.p_rst_pos(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--增加销售分钟和周几字段
delete from rst.rst_pos where sale_date=$1;
insert into rst.rst_pos
(
	shop_code,
	product_code,
	product_name,
	doc_no,
	sale_date,
	sale_time,
	sale_min,
	week,
	qty,
	amt_origin,
	amt,
	measure_unit
)
select 
	a.shop_code,
	a.product_code,
	b.product_name,
	a.doc_no,
	a.sale_date,
	a.sale_time,
	extract(min from a.sale_time)+60*
		(
			case when extract(hour from a.sale_time)=0 then 24
				when extract(hour from a.sale_time)=1 then 25 
				when extract(hour from a.sale_time)=2 then 26
				else extract(hour from a.sale_time) 
			end
		) sale_min,
	extract(dow from a.sale_date) week,
	a.qty,
	round(a.qty*a.price_origin,2) amt_origin,
	a.amt amt,
	b.measure_unit
from edw.fct_pos_detail a
left join edw.dim_product b 
	on a.product_code=b.product_code
where a.sale_date=$1
;
	RETURN 1;
END

$function$
;

```