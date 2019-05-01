# management_analiys
```
CREATE OR REPLACE FUNCTION rst.p_rst_management_analysis(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--每家门店每天的营业额
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t1.shop_code,
	t3.shop_name,
	t1.sale_date,
	sum(t1.qty*coalesce(t2.price_sale,0)) sale_amt
from edw.fct_pos_detail t1
left join edw.dim_price t2 
	on t1.shop_code=t2.shop_code 
	and t1.product_code=t2.product_code
left join edw.dim_shop t3 
	on t1.shop_code=t3.shop_code
where t1.sale_date=$1
group by 
	t1.shop_code,
	t3.shop_name,
	t1.sale_date
;


--每家门店每天的客单数
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	shop_code,
	sale_date,
	doc_no
from edw.fct_pos
where sale_date=$1
group by 
	shop_code,
	sale_date,
	doc_no
;


drop table if exists tmp3;
create temporary table tmp3 as 
select 
	shop_code,
	sale_date,
	count(*) customer_qty
from tmp2
where sale_date=$1
group by 
	shop_code,
	sale_date
;


--每家店去年同期是否有销售额
drop table if exists tmp4;
create temporary table tmp4 as 
select 
	shop_code,
	sale_date+'1 year'::interval sale_date,
	'1' is_sale
from rst.rst_pos 
where sale_date=($1-'1 year'::interval)::date
group by 
	shop_code,
	sale_date+'1 year'::interval
;


--汇总，计算客单价
delete from rst.rst_management_analysis where sale_date=$1;
insert into rst.rst_management_analysis
(
	shop_code,
	shop_name,
	sale_date,
	sale_year,
	sale_week,
	sale_amt,
	customer_qty,
	customer_price,
	is_sale
)  
select 
	t4.shop_code,
	t4.shop_name,
	t4.sale_date,
	extract(year from date_trunc('week',t4.sale_date)) sale_year,
	extract(week from date_trunc('week',t4.sale_date)) sale_week,
	t4.sale_amt,
	coalesce(t6.customer_qty,0) customer_qty,
	t4.sale_amt/coalesce(t6.customer_qty,0) customer_price,
	coalesce(t7.is_sale,'0') is_sale
from tmp1 t4 
left join tmp3 t6 
	on t4.shop_code=t6.shop_code 
	and t4.sale_date=t6.sale_date
left join tmp4 t7 
	on t4.shop_code=t7.shop_code 
	and t4.sale_date=t7.sale_date
;

drop table if exists tmp1;
drop table if exists tmp2;
drop table if exists tmp3;
drop table if exists tmp4;

	RETURN 1;
END

$function$
;

```