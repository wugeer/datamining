# turnover_feat_recent
```
CREATE OR REPLACE FUNCTION rst.p_rst_turnover_feat_recent(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--取同门店近30天营业额
drop table if exists tmp_d;
create temporary table tmp_d as 
select 
	row_number() over(partition by t1.shop_code,t1.date_case order by t1.sale_date desc) date_id,
	t1.shop_code,
	t1.sale_date,
	t1.date_case,
	t1.turnover
from rst.rst_turnover_feat t1 
inner join rst.rst_shop_pos_inaccurate t2 
	on t1.shop_code=t2.shop_code
where t1.sale_date<$1 
	and t1.turnover is not null
;

drop table if exists tmp32;
create temporary table tmp32 as 
select 
	shop_code,
	sale_date,
	date_case,
	turnover
from tmp_d
where date_id<=30
;
--select * from tmp32


--计算上下限
drop table if exists tmp_f;
create temporary table tmp_f as 
select 
	(percentile_cont(0.25) within group(order by turnover))-3*((percentile_cont(0.75) within group(order by turnover))-(percentile_cont(0.25) within group(order by turnover))) low,
	(percentile_cont(0.75) within group(order by turnover))+3*((percentile_cont(0.75) within group(order by turnover))-(percentile_cont(0.25) within group(order by turnover))) high,
	shop_code,
	date_case
from tmp32
group by 
	shop_code,
	date_case
;

--剔除异常值
drop table if exists tmp35;
create temporary table tmp35 as 
select 
	d.shop_code,
	d.sale_date,
	d.date_case,
	d.turnover
from tmp32 d 
left join tmp_f f 
	on d.shop_code=f.shop_code 
	and d.date_case=f.date_case
where d.turnover>=f.low 
	and d.turnover<=f.high
;

--取同门店近3天营业额平均值
drop table if exists tmp_g;
create temporary table tmp_g as 
select 
	row_number() over(partition by shop_code,date_case order by sale_date desc) date_id,
	shop_code,
	sale_date,
	date_case,
	turnover
from tmp35
where sale_date<$1
;

drop table if exists tmp36;
create temporary table tmp36 as 
select 
	shop_code,
	date_case,
	avg(turnover) recent_3
from tmp_g
where date_id<=3
group by 
	shop_code,
	date_case
;


--取同门店近7天营业额平均值
drop table if exists tmp37;
create temporary table tmp37 as 
select 
	shop_code,
	date_case,
	avg(turnover) recent_7
from tmp_g
where date_id<=7
group by 	
	shop_code,
	date_case
;


--取在售门店及当天case
drop table if exists tmp_h;
create temporary table tmp_h as 
select 
	t13.shop_code, 
	t13.sale_date, 
	t13.date_case, 
	t13.turnover, 
	t13.pre, 
	t13.temp
from rst.rst_turnover_feat t13
inner join rst.rst_shop_pos_inaccurate t14 
	on t13.shop_code=t14.shop_code 
where t13.sale_date=$1
;


--汇总
drop table if exists tmp38;
create temporary table tmp38 as 
select 
	t8.shop_code,
	t8.sale_date,
	t8.date_case,
	t8.turnover,
	t8.pre,
	t8.temp,
	t10.recent_3,
	t11.recent_7
from tmp_h t8
left join tmp36 t10 
	on t8.shop_code=t10.shop_code 
	and t8.date_case=t10.date_case 
left join tmp37 t11 
	on t8.shop_code=t11.shop_code 
	and t8.date_case=t11.date_case 
where t8.sale_date=$1
;


--插入数据
delete from rst.rst_turnover_feat t1
using rst.rst_shop_pos_inaccurate t2 
where t1.shop_code=t2.shop_code
	and t1.sale_date=$1
;
insert into rst.rst_turnover_feat
(
	shop_code,
	sale_date,
	date_case,
	turnover,
	pre,
	"temp",
	recent_3,
	recent_7
)
select 
	shop_code,
	sale_date,
	date_case,
	turnover,
	pre,
	"temp",
	recent_3,
	recent_7
from tmp38
;

drop table if exists tmp30;
drop table if exists tmp31;
drop table if exists tmp32;
drop table if exists tmp33;
drop table if exists tmp34;
drop table if exists tmp35;
drop table if exists tmp36;
drop table if exists tmp37;
drop table if exists tmp38;
drop table if exists tmp_d;
drop table if exists tmp_e;
drop table if exists tmp_f;
drop table if exists tmp_g;
drop table if exists tmp_h;

	RETURN 1;
END

$function$
;

```