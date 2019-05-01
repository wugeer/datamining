# sale_feat_recent
```
CREATE OR REPLACE FUNCTION rst.p_rst_sale_feat_recent(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--得到每家门店的当天在售商品，以及商品销量
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t1.shop_code,
	t1.product_code,
	t1.sale_date,
	t1.date_case,
	t1.turnover,
	t1.temp,
	t1.qty,
	t1.recent_3,
	t1.recent_7
from rst.rst_sale_feat t1
inner join rst.rst_shop_pos_inaccurate t2 
	on t1.shop_code=t2.shop_code
where sale_date=$1 
;
--select distinct shop_code from tmp_b


--筛选同case的近30天商品
drop table if exists tmp_b;
create temporary table tmp_b as 
select 
	row_number() over(partition by t1.shop_code,t1.product_code,t1.date_case order by t1.sale_date desc) date_id,
	t1.shop_code,
	t1.product_code,
	t1.sale_date,
	t1.date_case,
	t1.qty
from rst.rst_sale_feat t1
inner join rst.rst_shop_pos_inaccurate t2
	on t1.shop_code=t2.shop_code
where t1.sale_date<=$1 
	and t1.qty is not null
;


drop table if exists tmp3;
create temporary table tmp3 as 
select 
	shop_code,
	product_code,
	sale_date,
	date_case,
	qty
from tmp_b 
where date_id<=30
;
--select * from tmp3


--计算上下限
drop table if exists tmp_c;
create temporary table tmp_c as 
select 
	(percentile_cont(0.25) within group(order by qty))-1.5*((percentile_cont(0.75) within group(order by qty))-(percentile_cont(0.25) within group(order by qty))) low,
	(percentile_cont(0.75) within group(order by qty))+1.5*((percentile_cont(0.75) within group(order by qty))-(percentile_cont(0.25) within group(order by qty))) high,
	shop_code,
	product_code,
	date_case
from tmp3
group by 
	shop_code,
	product_code,
	date_case
;

--剔除异常值
drop table if exists tmp4;
create temporary table tmp4 as 
select 
	t6.shop_code,
	t6.product_code,
	t6.sale_date,
	t6.date_case,
	t6.qty
from tmp3 t6
left join tmp_c t7 
	on t6.shop_code=t7.shop_code 
	and t6.product_code=t7.product_code 
	and t6.date_case=t7.date_case
where t6.qty>=t7.low and t6.qty<=t7.high
;
--select * from tmp4


--求出（不含当天）近三天同case商品平均销量
drop table if exists tmp_d;
create temporary table tmp_d as 
select 
	row_number() over(partition by shop_code,product_code,date_case order by sale_date desc) date_id,
	shop_code,
	product_code,
	sale_date,
	date_case,
	qty
from tmp4
where sale_date<$1
;


drop table if exists tmp5;
create temporary table tmp5 as 
select 
	shop_code,
	product_code,
	date_case,
	avg(qty) recent_3
from tmp_d
where date_id<=3
group by 
	shop_code,
	product_code,
	date_case
;
--select distinct shop_code from tmp9 


--求出（不含当天）近七天同case商品平均销量
drop table if exists tmp6;
create temporary table tmp6 as 
select 
	shop_code,
	product_code,
	date_case,
	avg(qty) recent_7
from tmp_d
where date_id<=7
group by 
	shop_code,
	product_code,
	date_case
;


--合并当天数据
drop table if exists tmp9;
create temporary table tmp9 as 
select 
	t8.shop_code,
	t8.product_code,
	t8.sale_date,
	t8.date_case,
	t8.turnover,
	t8.temp,
	t8.qty,
	t9.recent_3,
	t10.recent_7
from tmp1 t8 
left join tmp5 t9 
	on t8.shop_code=t9.shop_code 
	and t8.product_code=t9.product_code 
	and t8.date_case=t9.date_case
left join tmp6 t10 
	on t8.shop_code=t10.shop_code 
	and t8.product_code=t10.product_code 
	and t8.date_case=t10.date_case
;
--select count(1) from tmp9

--插入当天数据
delete from rst.rst_sale_feat t1
using rst.rst_shop_pos_inaccurate t2 
where t1.shop_code=t2.shop_code
	and t1.sale_date=$1
;


insert into rst.rst_sale_feat 
(
	shop_code,
	product_code,
	sale_date,
	date_case,
	turnover,
	temp,
	qty,
	recent_3,
	recent_7
)
select 
	shop_code,
	product_code,
	sale_date,
	date_case,
	turnover,
	temp,
	qty,
	recent_3,
	recent_7
from tmp9
;


drop table if exists tmp1;
drop table if exists tmp2;
drop table if exists tmp3;
drop table if exists tmp4;
drop table if exists tmp5;
drop table if exists tmp6;
drop table if exists tmp7;
drop table if exists tmp8;
drop table if exists tmp9;
drop table if exists tmp10;
drop table if exists tmp_b;
drop table if exists tmp_c;
drop table if exists tmp_d;
drop table if exists tmp_e;

	RETURN 1;
END

$function$
;

```