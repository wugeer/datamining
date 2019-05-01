# turnover_feat_avg
```
CREATE OR REPLACE FUNCTION rst.p_rst_turnover_feat_avg(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--取当天在售门店
drop table if exists tmp0;
create temporary table tmp0 as 
select 
	distinct shop_code
from rst.rst_product_onsale
;

--增量插入turnover_feat，全量要删开店前日期
delete from rst.rst_turnover_feat where sale_date=$1;
insert into rst.rst_turnover_feat
(
	shop_code,
	sale_date,
	date_case,
	turnover
)
select 
	t8.shop_code,
	t8."date" sale_date,
	t8."case" date_case,
	sum(t10.sale_total_qty*coalesce(t11.price_sale,0)) turnover
from rst.rst_shop_date_case t8
inner join tmp0 t9 
	on t8.shop_code=t9.shop_code
left join rst.rst_inventory t10 
	on t8.shop_code=t10.shop_code 
	and t8."date"=t10.check_date
left join edw.dim_price t11 
	on t10.shop_code=t11.shop_code 
	and t10.product_code=t11.product_code
where t8."date"=$1
group by 
	t8.shop_code,
	t8."date",
	t8."case"
;

--有目标营业额的门店，营业额改为空
update rst.rst_turnover_feat t1
set turnover=null 
from rst.rst_target_turnover t2 
where t1.shop_code=t2.shop_code
	and t1.sale_date=t2.sale_date
	and t2.sale_date=$1
;

update rst.rst_turnover_feat t1 
set turnover=null 
from rst.rst_target_product_turnover t2 
where t1.shop_code=t2.shop_code
	and t1.sale_date=t2.sale_date
	and t2.sale_date=$1
;

--=================================================================================

--计算在营门店每个小时的营业额
drop table if exists tmp_a;
create temporary table tmp_a as 
select 
	t1.shop_code,
	t1."date" sale_date,
	case when extract(hour from t2.sale_time)=0 then 24
		when extract(hour from t2.sale_time)=1 then 25
		when extract(hour from t2.sale_time)=2 then 26
		else extract(hour from t2.sale_time) 
	end sale_hour,
	t2.qty*coalesce(t3.price_sale,0) amt
from rst.rst_shop_date_case t1
inner join tmp0 t0
	on t1.shop_code=t0.shop_code
left join rst.rst_pos t2 
	on t1.shop_code=t2.shop_code 
	and t1."date"=t2.sale_date
left join edw.dim_price t3 
	on t2.shop_code=t3.shop_code 
	and t2.product_code=t3.product_code 
where t1."date"=$1
;

drop table if exists tmp1;
create temporary table tmp1 as 
select 
	shop_code,
	sale_date,
	sale_hour,
	sum(coalesce(amt,0)) turnover
from tmp_a 
group by 
	shop_code,
	sale_date,
	sale_hour
;
--select * from tmp1

--计算在营门店每个小时的营业额累计
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	shop_code,
	sale_date,
	coalesce(sum(case when sale_hour>=3 then turnover end),0) sum_3_turnover,
	coalesce(sum(case when sale_hour>=4 then turnover end),0) sum_4_turnover,
	coalesce(sum(case when sale_hour>=5 then turnover end),0) sum_5_turnover,
	coalesce(sum(case when sale_hour>=6 then turnover end),0) sum_6_turnover,
	coalesce(sum(case when sale_hour>=7 then turnover end),0) sum_7_turnover,
	coalesce(sum(case when sale_hour>=8 then turnover end),0) sum_8_turnover,
	coalesce(sum(case when sale_hour>=9 then turnover end),0) sum_9_turnover,
	coalesce(sum(case when sale_hour>=10 then turnover end),0) sum_10_turnover,
	coalesce(sum(case when sale_hour>=11 then turnover end),0) sum_11_turnover,
	coalesce(sum(case when sale_hour>=12 then turnover end),0) sum_12_turnover,
	coalesce(sum(case when sale_hour>=13 then turnover end),0) sum_13_turnover,
	coalesce(sum(case when sale_hour>=14 then turnover end),0) sum_14_turnover,
	coalesce(sum(case when sale_hour>=15 then turnover end),0) sum_15_turnover,
	coalesce(sum(case when sale_hour>=16 then turnover end),0) sum_16_turnover,
	coalesce(sum(case when sale_hour>=17 then turnover end),0) sum_17_turnover,
	coalesce(sum(case when sale_hour>=18 then turnover end),0) sum_18_turnover,
	coalesce(sum(case when sale_hour>=19 then turnover end),0) sum_19_turnover,
	coalesce(sum(case when sale_hour>=20 then turnover end),0) sum_20_turnover,
	coalesce(sum(case when sale_hour>=21 then turnover end),0) sum_21_turnover,
	coalesce(sum(case when sale_hour>=22 then turnover end),0) sum_22_turnover,
	coalesce(sum(case when sale_hour>=23 then turnover end),0) sum_23_turnover,
	coalesce(sum(case when sale_hour>=24 then turnover end),0) sum_0_turnover,
	coalesce(sum(case when sale_hour>=25 then turnover end),0) sum_1_turnover,
	coalesce(sum(case when sale_hour>=26 then turnover end),0) sum_2_turnover
from tmp1
group by 
	shop_code,
	sale_date
;
--select * from tmp2

--插入数据
delete from rst.rst_turnover_feat_sum where sale_date=$1;
insert into rst.rst_turnover_feat_sum
(
	shop_code,
	sale_date,
	time_3_turnover,
	time_4_turnover,
	time_5_turnover,
	time_6_turnover,
	time_7_turnover,
	time_8_turnover,
	time_9_turnover,
	time_10_turnover,
	time_11_turnover,
	time_12_turnover,
	time_13_turnover,
	time_14_turnover,
	time_15_turnover,
	time_16_turnover,
	time_17_turnover,
	time_18_turnover,
	time_19_turnover,
	time_20_turnover,
	time_21_turnover,
	time_22_turnover,
	time_23_turnover,
	time_0_turnover,
	time_1_turnover,
	time_2_turnover
)
select 
	shop_code,
	sale_date,
	sum_3_turnover-sum_4_turnover time_3_turnover,
	sum_4_turnover-sum_5_turnover time_4_turnover,
	sum_5_turnover-sum_6_turnover time_5_turnover,
	sum_6_turnover-sum_7_turnover time_6_turnover,
	sum_7_turnover-sum_8_turnover time_7_turnover,
	sum_8_turnover-sum_9_turnover time_8_turnover,
	sum_9_turnover-sum_10_turnover time_9_turnover,
	sum_10_turnover-sum_11_turnover time_10_turnover,
	sum_11_turnover-sum_12_turnover time_11_turnover,
	sum_12_turnover-sum_13_turnover time_12_turnover,
	sum_13_turnover-sum_14_turnover time_13_turnover,
	sum_14_turnover-sum_15_turnover time_14_turnover,
	sum_15_turnover-sum_16_turnover time_15_turnover,
	sum_16_turnover-sum_17_turnover time_16_turnover,
	sum_17_turnover-sum_18_turnover time_17_turnover,
	sum_18_turnover-sum_19_turnover time_18_turnover,
	sum_19_turnover-sum_20_turnover time_19_turnover,
	sum_20_turnover-sum_21_turnover time_20_turnover,
	sum_21_turnover-sum_22_turnover time_21_turnover,
	sum_22_turnover-sum_23_turnover time_22_turnover,
	sum_23_turnover-sum_0_turnover time_23_turnover,
	sum_0_turnover-sum_1_turnover time_0_turnover,
	sum_1_turnover-sum_2_turnover time_1_turnover,
	sum_2_turnover time_2_turnover
from tmp2
;
--select * from tmp3

--对每个小时营业额剔除异常值（一年内）
drop table if exists tmp4;
create temporary table tmp4 as 
with b3 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_3_turnover))-3*((percentile_cont(0.75) within group(order by time_3_turnover))-(percentile_cont(0.25) within group(order by time_3_turnover))) low,
		(percentile_cont(0.75) within group(order by time_3_turnover))+3*((percentile_cont(0.75) within group(order by time_3_turnover))-(percentile_cont(0.25) within group(order by time_3_turnover))) high,
		shop_code
	from rst.rst_turnover_feat_sum
	where sale_date>=($1-'1 year'::interval)::date 
		and sale_date<=$1
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	case when t4.time_3_turnover>=b3.low and t4.time_3_turnover<=b3.high then t4.time_3_turnover
		 else null 
	end time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from rst.rst_turnover_feat_sum t4
left join b3 on t4.shop_code=b3.shop_code
where t4.sale_date>=($1-'1 year'::interval)::date 
	and t4.sale_date<=$1
;
--select * from tmp4

drop table if exists tmp5;
create temporary table tmp5 as 
with b4 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_4_turnover))-3*((percentile_cont(0.75) within group(order by time_4_turnover))-(percentile_cont(0.25) within group(order by time_4_turnover))) low,
		(percentile_cont(0.75) within group(order by time_4_turnover))+3*((percentile_cont(0.75) within group(order by time_4_turnover))-(percentile_cont(0.25) within group(order by time_4_turnover))) high,
		shop_code
	from tmp4
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	case when t4.time_4_turnover>=b4.low and t4.time_4_turnover<=b4.high then t4.time_4_turnover
		 else null 
	end time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp4 t4
left join b4 on t4.shop_code=b4.shop_code
;

drop table if exists tmp6;
create temporary table tmp6 as 
with b5 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_5_turnover))-3*((percentile_cont(0.75) within group(order by time_5_turnover))-(percentile_cont(0.25) within group(order by time_5_turnover))) low,
		(percentile_cont(0.75) within group(order by time_5_turnover))+3*((percentile_cont(0.75) within group(order by time_5_turnover))-(percentile_cont(0.25) within group(order by time_5_turnover))) high,
		shop_code
	from tmp5
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	case when t4.time_5_turnover>=b5.low and t4.time_5_turnover<=b5.high then t4.time_5_turnover
		 else null 
	end time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp5 t4
left join b5 on t4.shop_code=b5.shop_code
;

drop table if exists tmp7;
create temporary table tmp7 as 
with b6 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_6_turnover))-3*((percentile_cont(0.75) within group(order by time_6_turnover))-(percentile_cont(0.25) within group(order by time_6_turnover))) low,
		(percentile_cont(0.75) within group(order by time_6_turnover))+3*((percentile_cont(0.75) within group(order by time_6_turnover))-(percentile_cont(0.25) within group(order by time_6_turnover))) high,
		shop_code
	from tmp6
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	case when t4.time_6_turnover>=b6.low and t4.time_6_turnover<=b6.high then t4.time_6_turnover
		 else null 
	end time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp6 t4
left join b6 on t4.shop_code=b6.shop_code
;

drop table if exists tmp8;
create temporary table tmp8 as 
with b7 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_7_turnover))-3*((percentile_cont(0.75) within group(order by time_7_turnover))-(percentile_cont(0.25) within group(order by time_7_turnover))) low,
		(percentile_cont(0.75) within group(order by time_7_turnover))+3*((percentile_cont(0.75) within group(order by time_7_turnover))-(percentile_cont(0.25) within group(order by time_7_turnover))) high,
		shop_code
	from tmp7
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	case when t4.time_7_turnover>=b7.low and t4.time_7_turnover<=b7.high then t4.time_7_turnover
		 else null 
	end time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp7 t4
left join b7 on t4.shop_code=b7.shop_code
;

drop table if exists tmp9;
create temporary table tmp9 as 
with b8 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_8_turnover))-3*((percentile_cont(0.75) within group(order by time_8_turnover))-(percentile_cont(0.25) within group(order by time_8_turnover))) low,
		(percentile_cont(0.75) within group(order by time_8_turnover))+3*((percentile_cont(0.75) within group(order by time_8_turnover))-(percentile_cont(0.25) within group(order by time_8_turnover))) high,
		shop_code
	from tmp8
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	case when t4.time_8_turnover>=b8.low and t4.time_8_turnover<=b8.high then t4.time_8_turnover
		 else null 
	end time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp8 t4
left join b8 on t4.shop_code=b8.shop_code
;

drop table if exists tmp10;
create temporary table tmp10 as 
with b9 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_9_turnover))-3*((percentile_cont(0.75) within group(order by time_9_turnover))-(percentile_cont(0.25) within group(order by time_9_turnover))) low,
		(percentile_cont(0.75) within group(order by time_9_turnover))+3*((percentile_cont(0.75) within group(order by time_9_turnover))-(percentile_cont(0.25) within group(order by time_9_turnover))) high,
		shop_code
	from tmp9
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	case when t4.time_9_turnover>=b9.low and t4.time_9_turnover<=b9.high then t4.time_9_turnover
		 else null 
	end time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp9 t4
left join b9 on t4.shop_code=b9.shop_code
;

drop table if exists tmp11;
create temporary table tmp11 as 
with b10 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_10_turnover))-3*((percentile_cont(0.75) within group(order by time_10_turnover))-(percentile_cont(0.25) within group(order by time_10_turnover))) low,
		(percentile_cont(0.75) within group(order by time_10_turnover))+3*((percentile_cont(0.75) within group(order by time_10_turnover))-(percentile_cont(0.25) within group(order by time_10_turnover))) high,
		shop_code
	from tmp10
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	case when t4.time_10_turnover>=b10.low and t4.time_10_turnover<=b10.high then t4.time_10_turnover
		 else null 
	end time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp10 t4
left join b10 on t4.shop_code=b10.shop_code
;

drop table if exists tmp12;
create temporary table tmp12 as 
with b11 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_11_turnover))-3*((percentile_cont(0.75) within group(order by time_11_turnover))-(percentile_cont(0.25) within group(order by time_11_turnover))) low,
		(percentile_cont(0.75) within group(order by time_11_turnover))+3*((percentile_cont(0.75) within group(order by time_11_turnover))-(percentile_cont(0.25) within group(order by time_11_turnover))) high,
		shop_code
	from tmp11
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	case when t4.time_11_turnover>=b11.low and t4.time_11_turnover<=b11.high then t4.time_11_turnover
		 else null 
	end time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp11 t4
left join b11 on t4.shop_code=b11.shop_code
;

drop table if exists tmp13;
create temporary table tmp13 as 
with b12 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_12_turnover))-3*((percentile_cont(0.75) within group(order by time_12_turnover))-(percentile_cont(0.25) within group(order by time_12_turnover))) low,
		(percentile_cont(0.75) within group(order by time_12_turnover))+3*((percentile_cont(0.75) within group(order by time_12_turnover))-(percentile_cont(0.25) within group(order by time_12_turnover))) high,
		shop_code
	from tmp12
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	case when t4.time_12_turnover>=b12.low and t4.time_12_turnover<=b12.high then t4.time_12_turnover
		 else null 
	end time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp12 t4
left join b12 on t4.shop_code=b12.shop_code
;

drop table if exists tmp14;
create temporary table tmp14 as 
with b13 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_13_turnover))-3*((percentile_cont(0.75) within group(order by time_13_turnover))-(percentile_cont(0.25) within group(order by time_13_turnover))) low,
		(percentile_cont(0.75) within group(order by time_13_turnover))+3*((percentile_cont(0.75) within group(order by time_13_turnover))-(percentile_cont(0.25) within group(order by time_13_turnover))) high,
		shop_code
	from tmp13
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	case when t4.time_13_turnover>=b13.low and t4.time_13_turnover<=b13.high then t4.time_13_turnover
		 else null 
	end time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp13 t4
left join b13 on t4.shop_code=b13.shop_code
;

drop table if exists tmp15;
create temporary table tmp15 as 
with b14 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_14_turnover))-3*((percentile_cont(0.75) within group(order by time_14_turnover))-(percentile_cont(0.25) within group(order by time_14_turnover))) low,
		(percentile_cont(0.75) within group(order by time_14_turnover))+3*((percentile_cont(0.75) within group(order by time_14_turnover))-(percentile_cont(0.25) within group(order by time_14_turnover))) high,
		shop_code
	from tmp14
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	case when t4.time_14_turnover>=b14.low and t4.time_14_turnover<=b14.high then t4.time_14_turnover
		 else null 
	end time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp14 t4
left join b14 on t4.shop_code=b14.shop_code
;

drop table if exists tmp16;
create temporary table tmp16 as 
with b15 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_15_turnover))-3*((percentile_cont(0.75) within group(order by time_15_turnover))-(percentile_cont(0.25) within group(order by time_15_turnover))) low,
		(percentile_cont(0.75) within group(order by time_15_turnover))+3*((percentile_cont(0.75) within group(order by time_15_turnover))-(percentile_cont(0.25) within group(order by time_15_turnover))) high,
		shop_code
	from tmp15
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	case when t4.time_15_turnover>=b15.low and t4.time_15_turnover<=b15.high then t4.time_15_turnover
		 else null 
	end time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp15 t4
left join b15 on t4.shop_code=b15.shop_code
;

drop table if exists tmp17;
create temporary table tmp17 as 
with b16 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_16_turnover))-3*((percentile_cont(0.75) within group(order by time_16_turnover))-(percentile_cont(0.25) within group(order by time_16_turnover))) low,
		(percentile_cont(0.75) within group(order by time_16_turnover))+3*((percentile_cont(0.75) within group(order by time_16_turnover))-(percentile_cont(0.25) within group(order by time_16_turnover))) high,
		shop_code
	from tmp16
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	case when t4.time_16_turnover>=b16.low and t4.time_16_turnover<=b16.high then t4.time_16_turnover
		 else null 
	end time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp16 t4
left join b16 on t4.shop_code=b16.shop_code
;

drop table if exists tmp18;
create temporary table tmp18 as 
with b17 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_17_turnover))-3*((percentile_cont(0.75) within group(order by time_17_turnover))-(percentile_cont(0.25) within group(order by time_17_turnover))) low,
		(percentile_cont(0.75) within group(order by time_17_turnover))+3*((percentile_cont(0.75) within group(order by time_17_turnover))-(percentile_cont(0.25) within group(order by time_17_turnover))) high,
		shop_code
	from tmp17
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	case when t4.time_17_turnover>=b17.low and t4.time_17_turnover<=b17.high then t4.time_17_turnover
		 else null 
	end time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp17 t4
left join b17 on t4.shop_code=b17.shop_code
;

drop table if exists tmp19;
create temporary table tmp19 as 
with b18 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_18_turnover))-3*((percentile_cont(0.75) within group(order by time_18_turnover))-(percentile_cont(0.25) within group(order by time_18_turnover))) low,
		(percentile_cont(0.75) within group(order by time_18_turnover))+3*((percentile_cont(0.75) within group(order by time_18_turnover))-(percentile_cont(0.25) within group(order by time_18_turnover))) high,
		shop_code
	from tmp18
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	case when t4.time_18_turnover>=b18.low and t4.time_18_turnover<=b18.high then t4.time_18_turnover
		 else null 
	end time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp18 t4
left join b18 on t4.shop_code=b18.shop_code
;

drop table if exists tmp20;
create temporary table tmp20 as 
with b19 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_19_turnover))-3*((percentile_cont(0.75) within group(order by time_19_turnover))-(percentile_cont(0.25) within group(order by time_19_turnover))) low,
		(percentile_cont(0.75) within group(order by time_19_turnover))+3*((percentile_cont(0.75) within group(order by time_19_turnover))-(percentile_cont(0.25) within group(order by time_19_turnover))) high,
		shop_code
	from tmp19
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	case when t4.time_19_turnover>=b19.low and t4.time_19_turnover<=b19.high then t4.time_19_turnover
		 else null 
	end time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp19 t4
left join b19 on t4.shop_code=b19.shop_code
;

drop table if exists tmp21;
create temporary table tmp21 as 
with b20 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_20_turnover))-3*((percentile_cont(0.75) within group(order by time_20_turnover))-(percentile_cont(0.25) within group(order by time_20_turnover))) low,
		(percentile_cont(0.75) within group(order by time_20_turnover))+3*((percentile_cont(0.75) within group(order by time_20_turnover))-(percentile_cont(0.25) within group(order by time_20_turnover))) high,
		shop_code
	from tmp20
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	case when t4.time_20_turnover>=b20.low and t4.time_20_turnover<=b20.high then t4.time_20_turnover
		 else null 
	end time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp20 t4
left join b20 on t4.shop_code=b20.shop_code
;

drop table if exists tmp22;
create temporary table tmp22 as 
with b21 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_21_turnover))-3*((percentile_cont(0.75) within group(order by time_21_turnover))-(percentile_cont(0.25) within group(order by time_21_turnover))) low,
		(percentile_cont(0.75) within group(order by time_21_turnover))+3*((percentile_cont(0.75) within group(order by time_21_turnover))-(percentile_cont(0.25) within group(order by time_21_turnover))) high,
		shop_code
	from tmp21
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	case when t4.time_21_turnover>=b21.low and t4.time_21_turnover<=b21.high then t4.time_21_turnover
		 else null 
	end time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp21 t4
left join b21 on t4.shop_code=b21.shop_code
;

drop table if exists tmp23;
create temporary table tmp23 as 
with b22 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_22_turnover))-3*((percentile_cont(0.75) within group(order by time_22_turnover))-(percentile_cont(0.25) within group(order by time_22_turnover))) low,
		(percentile_cont(0.75) within group(order by time_22_turnover))+3*((percentile_cont(0.75) within group(order by time_22_turnover))-(percentile_cont(0.25) within group(order by time_22_turnover))) high,
		shop_code
	from tmp22
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	case when t4.time_22_turnover>=b22.low and t4.time_22_turnover<=b22.high then t4.time_22_turnover
		 else null 
	end time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp22 t4
left join b22 on t4.shop_code=b22.shop_code
;

drop table if exists tmp24;
create temporary table tmp24 as 
with b23 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_23_turnover))-3*((percentile_cont(0.75) within group(order by time_23_turnover))-(percentile_cont(0.25) within group(order by time_23_turnover))) low,
		(percentile_cont(0.75) within group(order by time_23_turnover))+3*((percentile_cont(0.75) within group(order by time_23_turnover))-(percentile_cont(0.25) within group(order by time_23_turnover))) high,
		shop_code
	from tmp23
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	case when t4.time_23_turnover>=b23.low and t4.time_23_turnover<=b23.high then t4.time_23_turnover
		 else null 
	end time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp23 t4
left join b23 on t4.shop_code=b23.shop_code
;

drop table if exists tmp25;
create temporary table tmp25 as 
with b0 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_0_turnover))-3*((percentile_cont(0.75) within group(order by time_0_turnover))-(percentile_cont(0.25) within group(order by time_0_turnover))) low,
		(percentile_cont(0.75) within group(order by time_0_turnover))+3*((percentile_cont(0.75) within group(order by time_0_turnover))-(percentile_cont(0.25) within group(order by time_0_turnover))) high,
		shop_code
	from tmp24
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	case when t4.time_0_turnover>=b0.low and t4.time_0_turnover<=b0.high then t4.time_0_turnover
		 else null 
	end time_0_turnover,
	t4.time_1_turnover,
	t4.time_2_turnover
from tmp24 t4
left join b0 on t4.shop_code=b0.shop_code
;

drop table if exists tmp26;
create temporary table tmp26 as 
with b1 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_1_turnover))-3*((percentile_cont(0.75) within group(order by time_1_turnover))-(percentile_cont(0.25) within group(order by time_1_turnover))) low,
		(percentile_cont(0.75) within group(order by time_1_turnover))+3*((percentile_cont(0.75) within group(order by time_1_turnover))-(percentile_cont(0.25) within group(order by time_1_turnover))) high,
		shop_code
	from tmp25
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	case when t4.time_1_turnover>=b1.low and t4.time_1_turnover<=b1.high then t4.time_1_turnover
		 else null 
	end time_1_turnover,
	t4.time_2_turnover
from tmp25 t4
left join b1 on t4.shop_code=b1.shop_code
;

drop table if exists tmp27;
create temporary table tmp27 as 
with b2 as 
(
	select 
		(percentile_cont(0.25) within group(order by time_2_turnover))-3*((percentile_cont(0.75) within group(order by time_2_turnover))-(percentile_cont(0.25) within group(order by time_2_turnover))) low,
		(percentile_cont(0.75) within group(order by time_2_turnover))+3*((percentile_cont(0.75) within group(order by time_2_turnover))-(percentile_cont(0.25) within group(order by time_2_turnover))) high,
		shop_code
	from tmp26
	group by shop_code
)
select 
	t4.shop_code,
	t4.sale_date,
	t4.time_3_turnover,
	t4.time_4_turnover,
	t4.time_5_turnover,
	t4.time_6_turnover,
	t4.time_7_turnover,
	t4.time_8_turnover,
	t4.time_9_turnover,
	t4.time_10_turnover,
	t4.time_11_turnover,
	t4.time_12_turnover,
	t4.time_13_turnover,
	t4.time_14_turnover,
	t4.time_15_turnover,
	t4.time_16_turnover,
	t4.time_17_turnover,
	t4.time_18_turnover,
	t4.time_19_turnover,
	t4.time_20_turnover,
	t4.time_21_turnover,
	t4.time_22_turnover,
	t4.time_23_turnover,
	t4.time_0_turnover,
	t4.time_1_turnover,
	case when t4.time_2_turnover>=b2.low and t4.time_2_turnover<=b2.high then t4.time_2_turnover
		 else null 
	end time_2_turnover
from tmp26 t4 
left join b2 on t4.shop_code=b2.shop_code
;
--select * from tmp27

--计算剔除异常值后的每小时平均营业额
drop table if exists tmp28;
create temporary table tmp28 as 
select 
	shop_code,
	avg(time_3_turnover) avg_3_turnover,
	avg(time_4_turnover) avg_4_turnover,
	avg(time_5_turnover) avg_5_turnover,
	avg(time_6_turnover) avg_6_turnover,
	avg(time_7_turnover) avg_7_turnover,
	avg(time_8_turnover) avg_8_turnover,
	avg(time_9_turnover) avg_9_turnover,
	avg(time_10_turnover) avg_10_turnover,
	avg(time_11_turnover) avg_11_turnover,
	avg(time_12_turnover) avg_12_turnover,
	avg(time_13_turnover) avg_13_turnover,
	avg(time_14_turnover) avg_14_turnover,
	avg(time_15_turnover) avg_15_turnover,
	avg(time_16_turnover) avg_16_turnover,
	avg(time_17_turnover) avg_17_turnover,
	avg(time_18_turnover) avg_18_turnover,
	avg(time_19_turnover) avg_19_turnover,
	avg(time_20_turnover) avg_20_turnover,
	avg(time_21_turnover) avg_21_turnover,
	avg(time_22_turnover) avg_22_turnover,
	avg(time_23_turnover) avg_23_turnover,
	avg(time_0_turnover) avg_0_turnover,
	avg(time_1_turnover) avg_1_turnover,
	avg(time_2_turnover) avg_2_turnover,
	case when avg(time_3_turnover)+avg(time_4_turnover)+avg(time_5_turnover)+avg(time_6_turnover)+avg(time_7_turnover)+avg(time_8_turnover)+avg(time_9_turnover)+avg(time_10_turnover)+avg(time_11_turnover)+avg(time_12_turnover)+avg(time_13_turnover)+avg(time_14_turnover)+avg(time_15_turnover)+avg(time_16_turnover)+avg(time_17_turnover)+avg(time_18_turnover)+avg(time_19_turnover)+avg(time_20_turnover)+avg(time_21_turnover)+avg(time_22_turnover)+avg(time_23_turnover)+avg(time_0_turnover)+avg(time_1_turnover)+avg(time_2_turnover)=0 then 1 
		 else avg(time_3_turnover)+avg(time_4_turnover)+avg(time_5_turnover)+avg(time_6_turnover)+avg(time_7_turnover)+avg(time_8_turnover)+avg(time_9_turnover)+avg(time_10_turnover)+avg(time_11_turnover)+avg(time_12_turnover)+avg(time_13_turnover)+avg(time_14_turnover)+avg(time_15_turnover)+avg(time_16_turnover)+avg(time_17_turnover)+avg(time_18_turnover)+avg(time_19_turnover)+avg(time_20_turnover)+avg(time_21_turnover)+avg(time_22_turnover)+avg(time_23_turnover)+avg(time_0_turnover)+avg(time_1_turnover)+avg(time_2_turnover) 
	end sum_avg
from tmp27
group by shop_code
;
--select * from tmp28


--插入数据
truncate table rst.rst_turnover_feat_avg;
insert into rst.rst_turnover_feat_avg  
(
	shop_code,
	per_3_turnover,
	per_4_turnover,
	per_5_turnover,
	per_6_turnover,
	per_7_turnover,
	per_8_turnover,
	per_9_turnover,
	per_10_turnover,
	per_11_turnover,
	per_12_turnover,
	per_13_turnover,
	per_14_turnover,
	per_15_turnover,
	per_16_turnover,
	per_17_turnover,
	per_18_turnover,
	per_19_turnover,
	per_20_turnover,
	per_21_turnover,
	per_22_turnover,
	per_23_turnover,
	per_0_turnover,
	per_1_turnover,
	per_2_turnover
)
select 
	shop_code,
	avg_3_turnover/sum_avg per_3_turnover,
	avg_4_turnover/sum_avg per_4_turnover,
	avg_5_turnover/sum_avg per_5_turnover,
	avg_6_turnover/sum_avg per_6_turnover,
	avg_7_turnover/sum_avg per_7_turnover,
	avg_8_turnover/sum_avg per_8_turnover,
	avg_9_turnover/sum_avg per_9_turnover,
	avg_10_turnover/sum_avg per_10_turnover,
	avg_11_turnover/sum_avg per_11_turnover,
	avg_12_turnover/sum_avg per_12_turnover,
	avg_13_turnover/sum_avg per_13_turnover,
	avg_14_turnover/sum_avg per_14_turnover,
	avg_15_turnover/sum_avg per_15_turnover,
	avg_16_turnover/sum_avg per_16_turnover,
	avg_17_turnover/sum_avg per_17_turnover,
	avg_18_turnover/sum_avg per_18_turnover,
	avg_19_turnover/sum_avg per_19_turnover,
	avg_20_turnover/sum_avg per_20_turnover,
	avg_21_turnover/sum_avg per_21_turnover,
	avg_22_turnover/sum_avg per_22_turnover,
	avg_23_turnover/sum_avg per_23_turnover,
	avg_0_turnover/sum_avg per_0_turnover,
	avg_1_turnover/sum_avg per_1_turnover,
	avg_2_turnover/sum_avg per_2_turnover
from tmp28 
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
drop table if exists tmp11;
drop table if exists tmp12;
drop table if exists tmp13;
drop table if exists tmp14;
drop table if exists tmp15;
drop table if exists tmp16;
drop table if exists tmp17;
drop table if exists tmp18;
drop table if exists tmp19;
drop table if exists tmp20;
drop table if exists tmp21;
drop table if exists tmp22;
drop table if exists tmp23;
drop table if exists tmp24;
drop table if exists tmp25;
drop table if exists tmp26;
drop table if exists tmp27;
drop table if exists tmp28;

	RETURN 1;
END

$function$
;

```