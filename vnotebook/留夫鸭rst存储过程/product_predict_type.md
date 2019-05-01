# product_predict_type
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_predict_type(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN
	
--计算每家门店每个商品出现次数
drop table if exists tmp1;
create temporary table tmp1 as 
select shop_code,
	product_code,
	count(*) times
from rst.rst_sale_feat
where sale_date<=$1 and sale_date>=($1-'1 year'::interval)::date
group by shop_code,
	product_code
;


--取出次数大于等于28次的商品
drop table if exists tmp2;
create temporary table tmp2 as 
select shop_code,
	product_code,
	'0' new_old_type
from tmp1 
where times>=28
;
--select * from tmp2


--判断新老品
drop table if exists tmp3;
create temporary table tmp3 as 
select t1.shop_code,
	t1.product_code,
	t2.new_old_type new_old_type_yesterday,
	case when t3.new_old_type is not null then t3.new_old_type
		else '1' 
	end new_old_type,
	case when t2.predict_type is not null then t2.predict_type
		 else '1'
	end predict_type
from rst.rst_product_onsale t1
left join rst.rst_product_predict_type t2 on t1.shop_code=t2.shop_code 
	and t1.product_code=t2.product_code 
	and t2.sale_date=($1-'1 day'::interval)::date
left join tmp2 t3 on t1.shop_code=t3.shop_code and t1.product_code=t3.product_code
;


--插入数据
delete from rst.rst_product_predict_type where sale_date=$1;
insert into rst.rst_product_predict_type
(
	shop_code,
	product_code,
	new_old_type_yesterday,
	new_old_type,
	predict_type,
	sale_date
)
select shop_code,
	product_code,
	new_old_type_yesterday,
	new_old_type,
	predict_type,
	$1
from tmp3
;


drop table if exists tmp1;
drop table if exists tmp2;
drop table if exists tmp3;

	RETURN 1;
END

$function$
;

```