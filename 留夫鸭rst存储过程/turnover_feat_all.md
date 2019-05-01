# turnover_feat_all
```
CREATE OR REPLACE FUNCTION rst.p_rst_turnover_feat_all()
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--取在售门店
drop table if exists tmp1;
create temporary table tmp1 as 
select distinct shop_code
from rst.rst_product_onsale
;

--计算营业额
drop table if exists tmp2;
create temporary table tmp2 as 
select t8.shop_code,
t9."date" sale_date,
t9."case" date_case,
sum(t10.sale_total_qty*COALESCE(t11.price_sale,0)) turnover
from tmp1 t8
left join rst.rst_shop_date_case t9 on t8.shop_code=t9.shop_code
left join rst.rst_inventory t10 on t9.shop_code=t10.shop_code and t9."date"=t10.check_date
left join edw.dim_price t11 on t10.shop_code=t11.shop_code and t10.product_code=t11.product_code
where t9."date">='2017-07-01'::date
group by t8.shop_code,
t9."date",
t9."case"
;

--取每家门店最早营业日期
drop table if exists tmp3;
create temporary table tmp3 as 
select shop_code,
min(sale_date) min_date
from edw.fct_pos_detail
group by shop_code
;

--取开店后日期
drop table if exists tmp4;
create temporary table tmp4 as 
select t1.shop_code,
t1.sale_date,
t1.date_case,
t1.turnover
from tmp2 t1
left join tmp3 t2 on t1.shop_code=t2.shop_code
where t1.sale_date>=t2.min_date
;

truncate table rst.rst_turnover_feat;
insert into rst.rst_turnover_feat
(
shop_code,
sale_date,
date_case,
turnover
)
select shop_code,
sale_date,
date_case,
turnover
from tmp4
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