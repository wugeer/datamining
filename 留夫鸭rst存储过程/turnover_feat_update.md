# turnover_feat_update
```
CREATE OR REPLACE FUNCTION rst.p_rst_turnover_feat_update(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN


--得到营业额占比与降雨量、温度乘积
drop table if exists tmp30;
create temporary table tmp30 as 
select 
	t5.shop_code,
	t7.temp_3*t5.per_3_turnover+t7.temp_4*t5.per_4_turnover+t7.temp_5*t5.per_5_turnover+t7.temp_6*t5.per_6_turnover+t7.temp_7*t5.per_7_turnover+t7.temp_8*t5.per_8_turnover+t7.temp_9*t5.per_9_turnover+t7.temp_10*t5.per_10_turnover+t7.temp_11*t5.per_11_turnover+t7.temp_12*t5.per_12_turnover+t7.temp_13*t5.per_13_turnover+t7.temp_14*t5.per_14_turnover+t7.temp_15*t5.per_15_turnover+t7.temp_16*t5.per_16_turnover+t7.temp_17*t5.per_17_turnover+t7.temp_18*t5.per_18_turnover+t7.temp_19*t5.per_19_turnover+t7.temp_20*t5.per_20_turnover+t7.temp_21*t5.per_21_turnover+t7.temp_22*t5.per_22_turnover+t7.temp_23*t5.per_23_turnover+t7.temp_0*t5.per_0_turnover+t7.temp_1*t5.per_1_turnover+t7.temp_2*t5.per_2_turnover temp_turnover,
	t7.pre_3*t5.per_3_turnover+t7.pre_4*t5.per_4_turnover+t7.pre_5*t5.per_5_turnover+t7.pre_6*t5.per_6_turnover+t7.pre_7*t5.per_7_turnover+t7.pre_8*t5.per_8_turnover+t7.pre_9*t5.per_9_turnover+t7.pre_10*t5.per_10_turnover+t7.pre_11*t5.per_11_turnover+t7.pre_12*t5.per_12_turnover+t7.pre_13*t5.per_13_turnover+t7.pre_14*t5.per_14_turnover+t7.pre_15*t5.per_15_turnover+t7.pre_16*t5.per_16_turnover+t7.pre_17*t5.per_17_turnover+t7.pre_18*t5.per_18_turnover+t7.pre_19*t5.per_19_turnover+t7.pre_20*t5.per_20_turnover+t7.pre_21*t5.per_21_turnover+t7.pre_22*t5.per_22_turnover+t7.pre_23*t5.per_23_turnover+t7.pre_0*t5.per_0_turnover+t7.pre_1*t5.per_1_turnover+t7.pre_2*t5.per_2_turnover pre_turnover
from rst.rst_turnover_feat_avg t5
left join rst.rst_shop t6 
	on t5.shop_code=t6.shop_code 
left join rst.rst_weather t7 
	on t6.city=t7.city 
	and t7.weather_date=$1
;
--select * from tmp30


--取同门店近30天营业额
drop table if exists tmp_d;
create temporary table tmp_d as 
select 
	row_number() over(partition by shop_code,date_case order by sale_date desc) date_id,
	shop_code,
	sale_date,
	date_case,
	turnover
from rst.rst_turnover_feat
where sale_date<$1 
	and turnover is not null
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
	(percentile_cont(0.25) within group(order by turnover))-1.5*((percentile_cont(0.75) within group(order by turnover))-(percentile_cont(0.25) within group(order by turnover))) low,
	(percentile_cont(0.75) within group(order by turnover))+1.5*((percentile_cont(0.75) within group(order by turnover))-(percentile_cont(0.25) within group(order by turnover))) high,
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
	distinct t13.shop_code,
	$1 sale_date,
	t14."case" date_case
from rst.rst_product_onsale t13
left join rst.rst_shop_date_case t14 
	on t13.shop_code=t14.shop_code 
	and t14."date"=$1
where t14."case" is not null
;


--汇总
drop table if exists tmp38;
create temporary table tmp38 as 
select 
	t8.shop_code,
	t8.sale_date,
	t8.date_case,
	t12.turnover,
	t9.pre_turnover pre,
	t9.temp_turnover "temp",
	t10.recent_3,
	t11.recent_7
from tmp_h t8
left join tmp30 t9 
	on t8.shop_code=t9.shop_code 
left join tmp36 t10 
	on t8.shop_code=t10.shop_code 
	and t8.date_case=t10.date_case 
left join tmp37 t11 
	on t8.shop_code=t11.shop_code 
	and t8.date_case=t11.date_case 
left join rst.rst_turnover_feat t12 
	on t8.shop_code=t12.shop_code 
	and t8.sale_date=t12.sale_date
where t8.sale_date=$1
;


--插入数据
delete from rst.rst_turnover_feat where sale_date=$1;
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