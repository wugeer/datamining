# sale_supply_avg_copy
```
CREATE OR REPLACE FUNCTION rst.p_rst_sale_supply_avg_copy(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--取所有在售商品
drop table if exists tmp0;
create temporary table tmp0 as 
select 
	distinct shop_code,
	product_code,
	$1 sale_date
from rst.rst_product_onsale
;

--取当天最大销售时间（剔除品尝赠送）
drop table if exists tmp1;
create temp table tmp1 as 
select 
	shop_code,
	product_code,
	sale_date,
	max(sale_min) max_min
from rst.rst_pos
where sale_date=$1
	and amt<>0
group by 
	shop_code,
	product_code,
	sale_date
;

--汇总（剔除品尝赠送）
drop table if exists tmp2;
create temp table tmp2 as 
select 
	t0.shop_code,
	t0.product_code,
	t0.sale_date,
	t4.sale_min,
	t4.qty,
	t1.max_min,
	t2."case" date_case,
	t3.inferential_end_qty end_qty
from tmp0 t0 
left join tmp1 t1 
	on t0.shop_code=t1.shop_code 
	and t0.product_code=t1.product_code 
	and t0.sale_date=t1.sale_date
left join rst.rst_shop_date_case t2 
	on t0.shop_code=t2.shop_code 
	and t0.sale_date=t2."date" 
left join rst.rst_inventory t3 
	on t0.shop_code=t3.shop_code 
	and t0.product_code=t3.product_code 
	and t0.sale_date=t3.check_date
left join rst.rst_pos t4 
	on t0.shop_code=t4.shop_code 
	and t0.product_code=t4.product_code 
	and t0.sale_date=t4.sale_date 
	and t4.amt<>0
;

delete from rst.rst_sale_supply_sum where sale_date=$1;
insert into rst.rst_sale_supply_sum 
(
	shop_code,
	product_code,
	date_case,
	sale_date,
	max_min,
	sum_3_sale,
	sum_3a_sale,
	sum_4_sale,
	sum_4a_sale,
	sum_5_sale,
	sum_5a_sale,
	sum_6_sale,
	sum_6a_sale,
	sum_7_sale,
	sum_7a_sale,
	sum_8_sale,
	sum_8a_sale,
	sum_9_sale,
	sum_9a_sale,
	sum_10_sale,
	sum_10a_sale,
	sum_11_sale,
	sum_11a_sale,
	sum_12_sale,
	sum_12a_sale,
	sum_13_sale,
	sum_13a_sale,
	sum_14_sale,
	sum_14a_sale,
	sum_15_sale,
	sum_15a_sale,
	sum_16_sale,
	sum_16a_sale,
	sum_17_sale,
	sum_17a_sale,
	sum_18_sale,
	sum_18a_sale,
	sum_19_sale,
	sum_19a_sale,
	sum_20_sale,
	sum_20a_sale,
	sum_21_sale,
	sum_21a_sale,
	sum_22_sale,
	sum_22a_sale,
	sum_23_sale,
	sum_23a_sale,
	sum_0_sale,
	sum_0a_sale,
	sum_1_sale,
	sum_1a_sale,
	sum_2_sale,
	sum_2a_sale,
	end_qty
)
select 
	shop_code,
	product_code,
	date_case,
	sale_date,
	coalesce(max_min,180) max_min,
	coalesce(sum(case when sale_min>=180 then qty end),0) sum_3_sale,
	sum(case when sale_min>=210 then qty end) sum_3a_sale,
	sum(case when sale_min>=240 then qty end) sum_4_sale,
	sum(case when sale_min>=270 then qty end) sum_4a_sale,
	sum(case when sale_min>=300 then qty end) sum_5_sale,
	sum(case when sale_min>=330 then qty end) sum_5a_sale,
	sum(case when sale_min>=360 then qty end) sum_6_sale,
	sum(case when sale_min>=390 then qty end) sum_6a_sale,
	sum(case when sale_min>=420 then qty end) sum_7_sale,
	sum(case when sale_min>=450 then qty end) sum_7a_sale,
	sum(case when sale_min>=480 then qty end) sum_8_sale,
	sum(case when sale_min>=510 then qty end) sum_8a_sale,
	sum(case when sale_min>=540 then qty end) sum_9_sale,
	sum(case when sale_min>=570 then qty end) sum_9a_sale,
	sum(case when sale_min>=600 then qty end) sum_10_sale,
	sum(case when sale_min>=630 then qty end) sum_10a_sale,
	sum(case when sale_min>=660 then qty end) sum_11_sale,
	sum(case when sale_min>=690 then qty end) sum_11a_sale,
	sum(case when sale_min>=720 then qty end) sum_12_sale,
	sum(case when sale_min>=750 then qty end) sum_12a_sale,
	sum(case when sale_min>=780 then qty end) sum_13_sale,
	sum(case when sale_min>=810 then qty end) sum_13a_sale,
	sum(case when sale_min>=840 then qty end) sum_14_sale,
	sum(case when sale_min>=870 then qty end) sum_14a_sale,
	sum(case when sale_min>=900 then qty end) sum_15_sale,
	sum(case when sale_min>=930 then qty end) sum_15a_sale,
	sum(case when sale_min>=960 then qty end) sum_16_sale,
	sum(case when sale_min>=990 then qty end) sum_16a_sale,
	sum(case when sale_min>=1020 then qty end) sum_17_sale,
	sum(case when sale_min>=1050 then qty end) sum_17a_sale,
	sum(case when sale_min>=1080 then qty end) sum_18_sale,
	sum(case when sale_min>=1110 then qty end) sum_18a_sale,
	sum(case when sale_min>=1140 then qty end) sum_19_sale,
	sum(case when sale_min>=1170 then qty end) sum_19a_sale,
	sum(case when sale_min>=1200 then qty end) sum_20_sale,
	sum(case when sale_min>=1230 then qty end) sum_20a_sale,
	sum(case when sale_min>=1260 then qty end) sum_21_sale,
	sum(case when sale_min>=1290 then qty end) sum_21a_sale,
	sum(case when sale_min>=1320 then qty end) sum_22_sale,
	sum(case when sale_min>=1350 then qty end) sum_22a_sale,
	sum(case when sale_min>=1380 then qty end) sum_23_sale,
	sum(case when sale_min>=1410 then qty end) sum_23a_sale,
	sum(case when sale_min>=1440 then qty end) sum_0_sale,
	sum(case when sale_min>=1470 then qty end) sum_0a_sale,
	sum(case when sale_min>=1500 then qty end) sum_1_sale,
	sum(case when sale_min>=1530 then qty end) sum_1a_sale,
	sum(case when sale_min>=1560 then qty end) sum_2_sale,
	sum(case when sale_min>=1590 then qty end) sum_2a_sale,
	end_qty
from tmp2 
group by 
	shop_code,
	product_code,
	date_case,
	sale_date,
	max_min,
	end_qty
;

--删除rst.rst_sale_supply_sum中7日没有销售或库存的商品
delete from rst.rst_sale_supply_sum t1
where not exists 
(
	select 1 from rst.rst_inventory t2
	where t1.shop_code=t2.shop_code
		and t1.product_code=t2.product_code
		and t1.sale_date=t2.check_date
)
	and t1.sale_date<=$1
;

/*
--找出上一次有销售或者库存记录的日期（用来删除增量）
drop table if exists tmp8;
create temporary table tmp8 as 
select 
	shop_code,
	product_code,
	$1 sale_date,
	max(sale_date::timestamp) before_date
from rst.rst_sale_supply_sum
where sale_date<=$1
	and (max_min<>180 or end_qty<>0)
group by 
	shop_code,
	product_code,
	$1
;

--找出两个日期间隔大于7天的商品
drop table if exists tmp9;
create temporary table tmp9 as 
select
	shop_code,
	product_code,
	sale_date,
	before_date
from tmp8 
where sale_date-before_date>='7 day'::interval
;

--删除连续7天以上没有销售的商品
delete from rst.rst_sale_supply_sum t9
using tmp9 t10
where (t9.sale_date<=t10.sale_date and t9.sale_date>t10.before_date)
	and t9.shop_code=t10.shop_code 
	and t9.product_code=t10.product_code
	and t9.max_min=180
;
*/


--选出一年内近100天同case商品
drop table if exists tmp_a;
create temporary table tmp_a as 
select 
	row_number() over(partition by shop_code,product_code,date_case order by sale_date desc) date_id,
	shop_code,
	product_code,
	date_case,
	sale_date,
	max_min,
	sum_3_sale,
	sum_3a_sale,
	sum_4_sale,
	sum_4a_sale,
	sum_5_sale,
	sum_5a_sale,
	sum_6_sale,
	sum_6a_sale,
	sum_7_sale,
	sum_7a_sale,
	sum_8_sale,
	sum_8a_sale,
	sum_9_sale,
	sum_9a_sale,
	sum_10_sale,
	sum_10a_sale,
	sum_11_sale,
	sum_11a_sale,
	sum_12_sale,
	sum_12a_sale,
	sum_13_sale,
	sum_13a_sale,
	sum_14_sale,
	sum_14a_sale,
	sum_15_sale,
	sum_15a_sale,
	sum_16_sale,
	sum_16a_sale,
	sum_17_sale,
	sum_17a_sale,
	sum_18_sale,
	sum_18a_sale,
	sum_19_sale,
	sum_19a_sale,
	sum_20_sale,
	sum_20a_sale,
	sum_21_sale,
	sum_21a_sale,
	sum_22_sale,
	sum_22a_sale,
	sum_23_sale,
	sum_23a_sale,
	sum_0_sale,
	sum_0a_sale,
	sum_1_sale,
	sum_1a_sale,
	sum_2_sale,
	sum_2a_sale,
	end_qty
from rst.rst_sale_supply_sum 
where sale_date<=$1 
	and sale_date>=($1-'1 year'::interval)::date
;

drop table if exists tmp3;
create temporary table tmp3 as
select 
	shop_code,
	product_code,
	date_case,
	sale_date,
	max_min,
	sum_3_sale,
	sum_3a_sale,
	sum_4_sale,
	sum_4a_sale,
	sum_5_sale,
	sum_5a_sale,
	sum_6_sale,
	sum_6a_sale,
	sum_7_sale,
	sum_7a_sale,
	sum_8_sale,
	sum_8a_sale,
	sum_9_sale,
	sum_9a_sale,
	sum_10_sale,
	sum_10a_sale,
	sum_11_sale,
	sum_11a_sale,
	sum_12_sale,
	sum_12a_sale,
	sum_13_sale,
	sum_13a_sale,
	sum_14_sale,
	sum_14a_sale,
	sum_15_sale,
	sum_15a_sale,
	sum_16_sale,
	sum_16a_sale,
	sum_17_sale,
	sum_17a_sale,
	sum_18_sale,
	sum_18a_sale,
	sum_19_sale,
	sum_19a_sale,
	sum_20_sale,
	sum_20a_sale,
	sum_21_sale,
	sum_21a_sale,
	sum_22_sale,
	sum_22a_sale,
	sum_23_sale,
	sum_23a_sale,
	sum_0_sale,
	sum_0a_sale,
	sum_1_sale,
	sum_1a_sale,
	sum_2_sale,
	sum_2a_sale,
	end_qty
from tmp_a 
where date_id<=100
;


--计算剔除异常值的上下限
drop table if exists tmp4;
create temporary table tmp4 as 
select 
	(percentile_cont(0.25) within group(order by sum_3_sale))-1.5*((percentile_cont(0.75) within group(order by sum_3_sale))-(percentile_cont(0.25) within group(order by sum_3_sale))) low,
	(percentile_cont(0.75) within group(order by sum_3_sale))+1.5*((percentile_cont(0.75) within group(order by sum_3_sale))-(percentile_cont(0.25) within group(order by sum_3_sale))) high, 
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
drop table if exists tmp5;
create temporary table tmp5 as 
select 
	t5.shop_code,
	t5.product_code,
	t5.date_case,
	t5.sale_date,
	t5.max_min,
	t5.sum_3_sale,
	t5.sum_3a_sale,
	t5.sum_4_sale,
	t5.sum_4a_sale,
	t5.sum_5_sale,
	t5.sum_5a_sale,
	t5.sum_6_sale,
	t5.sum_6a_sale,
	t5.sum_7_sale,
	t5.sum_7a_sale,
	t5.sum_8_sale,
	t5.sum_8a_sale,
	t5.sum_9_sale,
	t5.sum_9a_sale,
	t5.sum_10_sale,
	t5.sum_10a_sale,
	t5.sum_11_sale,
	t5.sum_11a_sale,
	t5.sum_12_sale,
	t5.sum_12a_sale,
	t5.sum_13_sale,
	t5.sum_13a_sale,
	t5.sum_14_sale,
	t5.sum_14a_sale,
	t5.sum_15_sale,
	t5.sum_15a_sale,
	t5.sum_16_sale,
	t5.sum_16a_sale,
	t5.sum_17_sale,
	t5.sum_17a_sale,
	t5.sum_18_sale,
	t5.sum_18a_sale,
	t5.sum_19_sale,
	t5.sum_19a_sale,
	t5.sum_20_sale,
	t5.sum_20a_sale,
	t5.sum_21_sale,
	t5.sum_21a_sale,
	t5.sum_22_sale,
	t5.sum_22a_sale,
	t5.sum_23_sale,
	t5.sum_23a_sale,
	t5.sum_0_sale,
	t5.sum_0a_sale,
	t5.sum_1_sale,
	t5.sum_1a_sale,
	t5.sum_2_sale,
	t5.sum_2a_sale,
	t5.end_qty
from tmp3 t5
left join tmp4 t6 
	on t5.shop_code=t6.shop_code 
	and t5.product_code=t6.product_code 
	and t5.date_case=t6.date_case
where t5.sum_3_sale>=t6.low 
	and t5.sum_3_sale<=t6.high
;

--（日末库存大于零）不断货商品的补0
drop table if exists tmp6;
create temporary table tmp6 as 
select 
	t1.shop_code,
	t1.product_code,
	t1.date_case,
	t1.sale_date,
	t1.max_min,
	case when t1.max_min>=180 and t1.max_min<210 then t1.sum_3_sale
		 else t1.sum_3_sale-t1.sum_3a_sale 
	end time_3_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<210 then 0
		 when t1.max_min>=210 and t1.max_min<240 then t1.sum_3a_sale
		 else t1.sum_3a_sale-t1.sum_4_sale 
	end time_3a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<240 then 0
		 when t1.max_min>=240 and t1.max_min<270 then t1.sum_4_sale
		 else t1.sum_4_sale-t1.sum_4a_sale 
	end time_4_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<270 then 0
		 when t1.max_min>=270 and t1.max_min<300 then t1.sum_4a_sale
		 else t1.sum_4a_sale-t1.sum_5_sale 
	end time_4a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<300 then 0
		 when t1.max_min>=300 and t1.max_min<330 then t1.sum_5_sale
		 else t1.sum_5_sale-t1.sum_5a_sale 
	end time_5_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<330 then 0
		 when t1.max_min>=330 and t1.max_min<360 then t1.sum_5a_sale
		 else t1.sum_5a_sale-t1.sum_6_sale 
	end time_5a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<360 then 0
		 when t1.max_min>=360 and t1.max_min<390 then t1.sum_6_sale
		 else t1.sum_6_sale-t1.sum_6a_sale 
	end time_6_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<390 then 0
		 when t1.max_min>=390 and t1.max_min<420 then t1.sum_6a_sale
		 else t1.sum_6a_sale-t1.sum_7_sale 
	end time_6a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<420 then 0
		 when t1.max_min>=420 and t1.max_min<450 then t1.sum_7_sale
		 else t1.sum_7_sale-t1.sum_7a_sale 
	end time_7_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<450 then 0
		 when t1.max_min>=450 and t1.max_min<480 then t1.sum_7a_sale
		 else t1.sum_7a_sale-t1.sum_8_sale 
	end time_7a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<480 then 0
		 when t1.max_min>=480 and t1.max_min<510 then t1.sum_8_sale
		 else t1.sum_8_sale-t1.sum_8a_sale 
	end time_8_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<510 then 0
		 when t1.max_min>=510 and t1.max_min<540 then t1.sum_8a_sale
		 else t1.sum_8a_sale-t1.sum_9_sale 
	end time_8a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<540 then 0
		 when t1.max_min>=540 and t1.max_min<570 then t1.sum_9_sale
		 else t1.sum_9_sale-t1.sum_9a_sale 
	end time_9_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<570 then 0
		 when t1.max_min>=570 and t1.max_min<600 then t1.sum_9a_sale
		 else t1.sum_9a_sale-t1.sum_10_sale 
	end time_9a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<600 then 0
		 when t1.max_min>=600 and t1.max_min<630 then t1.sum_10_sale
		 else t1.sum_10_sale-t1.sum_10a_sale 
	end time_10_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<630 then 0
		 when t1.max_min>=630 and t1.max_min<660 then t1.sum_10a_sale
		 else t1.sum_10a_sale-t1.sum_11_sale 
	end time_10a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<660 then 0
		 when t1.max_min>=660 and t1.max_min<690 then t1.sum_11_sale
		 else t1.sum_11_sale-t1.sum_11a_sale 
	end time_11_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<690 then 0
		 when t1.max_min>=690 and t1.max_min<720 then t1.sum_11a_sale
		 else t1.sum_11a_sale-t1.sum_12_sale 
	end time_11a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<720 then 0
		 when t1.max_min>=720 and t1.max_min<750 then t1.sum_12_sale
		 else t1.sum_12_sale-t1.sum_12a_sale 
	end time_12_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<750 then 0
		 when t1.max_min>=750 and t1.max_min<780 then t1.sum_12a_sale
		 else t1.sum_12a_sale-t1.sum_13_sale 
	end time_12a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<780 then 0
		 when t1.max_min>=780 and t1.max_min<810 then t1.sum_13_sale
		 else t1.sum_13_sale-t1.sum_13a_sale 
	end time_13_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<810 then 0
		 when t1.max_min>=810 and t1.max_min<840 then t1.sum_13a_sale
		 else t1.sum_13a_sale-t1.sum_14_sale 
	end time_13a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<840 then 0
		 when t1.max_min>=840 and t1.max_min<870 then t1.sum_14_sale
		 else t1.sum_14_sale-t1.sum_14a_sale 
	end time_14_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<870 then 0
		 when t1.max_min>=870 and t1.max_min<900 then t1.sum_14a_sale
		 else t1.sum_14a_sale-t1.sum_15_sale 
	end time_14a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<900 then 0
		 when t1.max_min>=900 and t1.max_min<930 then t1.sum_15_sale
		 else t1.sum_15_sale-t1.sum_15a_sale 
	end time_15_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<930 then 0
		 when t1.max_min>=930 and t1.max_min<960 then t1.sum_15a_sale
		 else t1.sum_15a_sale-t1.sum_16_sale 
	end time_15a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<960 then 0
		 when t1.max_min>=960 and t1.max_min<990 then t1.sum_16_sale
		 else t1.sum_16_sale-t1.sum_16a_sale 
	end time_16_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<990 then 0
		 when t1.max_min>=990 and t1.max_min<1020 then t1.sum_16a_sale
		 else t1.sum_16a_sale-t1.sum_17_sale 
	end time_16a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1020 then 0
		 when t1.max_min>=1020 and t1.max_min<1050 then t1.sum_17_sale
		 else t1.sum_17_sale-t1.sum_17a_sale 
	end time_17_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1050 then 0
		 when t1.max_min>=1050 and t1.max_min<1080 then t1.sum_17a_sale
		 else t1.sum_17a_sale-t1.sum_18_sale 
	end time_17a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1080 then 0
		 when t1.max_min>=1080 and t1.max_min<1110 then t1.sum_18_sale
		 else t1.sum_18_sale-t1.sum_18a_sale 
	end time_18_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1110 then 0
		 when t1.max_min>=1110 and t1.max_min<1140 then t1.sum_18a_sale
		 else t1.sum_18a_sale-t1.sum_19_sale 
	end time_18a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1140 then 0
		 when t1.max_min>=1140 and t1.max_min<1170 then t1.sum_19_sale
		 else t1.sum_19_sale-t1.sum_19a_sale 
	end time_19_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1170 then 0
		 when t1.max_min>=1170 and t1.max_min<1200 then t1.sum_19a_sale
		 else t1.sum_19a_sale-t1.sum_20_sale 
	end time_19a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1200 then 0
		 when t1.max_min>=1200 and t1.max_min<1230 then t1.sum_20_sale
		 else t1.sum_20_sale-t1.sum_20a_sale 
	end time_20_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1230 then 0
		 when t1.max_min>=1230 and t1.max_min<1260 then t1.sum_20a_sale
		 else t1.sum_20a_sale-t1.sum_21_sale 
	end time_20a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1260 then 0
		 when t1.max_min>=1260 and t1.max_min<1290 then t1.sum_21_sale
		 else t1.sum_21_sale-t1.sum_21a_sale 
	end time_21_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1290 then 0
		 when t1.max_min>=1290 and t1.max_min<1320 then t1.sum_21a_sale
		 else t1.sum_21a_sale-t1.sum_22_sale 
	end time_21a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1320 then 0
		 when t1.max_min>=1320 and t1.max_min<1350 then t1.sum_22_sale
		 else t1.sum_22_sale-t1.sum_22a_sale 
	end time_22_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1350 then 0
		 when t1.max_min>=1350 and t1.max_min<1380 then t1.sum_22a_sale
		 else t1.sum_22a_sale-t1.sum_23_sale 
	end time_22a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1380 then 0
		 when t1.max_min>=1380 and t1.max_min<1410 then t1.sum_23_sale
		 else t1.sum_23_sale-t1.sum_23a_sale 
	end time_23_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1410 then 0
		 when t1.max_min>=1410 and t1.max_min<1440 then t1.sum_23a_sale
		 else t1.sum_23a_sale-t1.sum_0_sale 
	end time_23a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1440 then 0
		 when t1.max_min>=1440 and t1.max_min<1470 then t1.sum_0_sale
		 else t1.sum_0_sale-t1.sum_0a_sale 
	end time_0_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1470 then 0
		 when t1.max_min>=1470 and t1.max_min<1500 then t1.sum_0a_sale
		 else t1.sum_0a_sale-t1.sum_1_sale 
	end time_0a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1500 then 0
		 when t1.max_min>=1500 and t1.max_min<1530 then t1.sum_1_sale
		 else t1.sum_1_sale-t1.sum_1a_sale 
	end time_1_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1530 then 0
		 when t1.max_min>=1530 and t1.max_min<1560 then t1.sum_1a_sale
		 else t1.sum_1a_sale-t1.sum_2_sale 
	end time_1a_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1560 then 0
		 when t1.max_min>=1560 and t1.max_min<1590 then t1.sum_2_sale
		 else t1.sum_2_sale-t1.sum_2a_sale 
	end time_2_sale,
	case when (t1.end_qty>0 or t2.loss_qty>0) and t1.max_min<1590 then 0
		 else t1.sum_2a_sale 
	end time_2a_sale,
	t1.end_qty
from tmp5 t1 
left join rst.rst_inventory t2 
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
	and t1.sale_date=t2.check_date
;


--取近30天同case商品
drop table if exists tmp_b;
create temporary table tmp_b as 
select 
	row_number() over(partition by shop_code,product_code,date_case order by sale_date desc) date_id,
	shop_code,
	product_code,
	date_case,
	sale_date,
	max_min,
	time_3_sale,
	time_3a_sale,
	time_4_sale,
	time_4a_sale,
	time_5_sale,
	time_5a_sale,
	time_6_sale,
	time_6a_sale,
	time_7_sale,
	time_7a_sale,
	time_8_sale,
	time_8a_sale,
	time_9_sale,
	time_9a_sale,
	time_10_sale,
	time_10a_sale,
	time_11_sale,
	time_11a_sale,
	time_12_sale,
	time_12a_sale,
	time_13_sale,
	time_13a_sale,
	time_14_sale,
	time_14a_sale,
	time_15_sale,
	time_15a_sale,
	time_16_sale,
	time_16a_sale,
	time_17_sale,
	time_17a_sale,
	time_18_sale,
	time_18a_sale,
	time_19_sale,
	time_19a_sale,
	time_20_sale,
	time_20a_sale,
	time_21_sale,
	time_21a_sale,
	time_22_sale,
	time_22a_sale,
	time_23_sale,
	time_23a_sale,
	time_0_sale,
	time_0a_sale,
	time_1_sale,
	time_1a_sale,
	time_2_sale,
	time_2a_sale,
	end_qty
from tmp6 
;

drop table if exists tmp7;
create temporary table tmp7 as 
select 
	shop_code,
	product_code,
	date_case,
	sale_date,
	max_min,
	time_3_sale,
	time_3a_sale,
	time_4_sale,
	time_4a_sale,
	time_5_sale,
	time_5a_sale,
	time_6_sale,
	time_6a_sale,
	time_7_sale,
	time_7a_sale,
	time_8_sale,
	time_8a_sale,
	time_9_sale,
	time_9a_sale,
	time_10_sale,
	time_10a_sale,
	time_11_sale,
	time_11a_sale,
	time_12_sale,
	time_12a_sale,
	time_13_sale,
	time_13a_sale,
	time_14_sale,
	time_14a_sale,
	time_15_sale,
	time_15a_sale,
	time_16_sale,
	time_16a_sale,
	time_17_sale,
	time_17a_sale,
	time_18_sale,
	time_18a_sale,
	time_19_sale,
	time_19a_sale,
	time_20_sale,
	time_20a_sale,
	time_21_sale,
	time_21a_sale,
	time_22_sale,
	time_22a_sale,
	time_23_sale,
	time_23a_sale,
	time_0_sale,
	time_0a_sale,
	time_1_sale,
	time_1a_sale,
	time_2_sale,
	time_2a_sale,
	end_qty
from tmp_b 
where date_id<=30
;

--计算销补平均值
delete from rst.rst_sale_supply_avg_copy where sale_date=$1;
insert into rst.rst_sale_supply_avg_copy
(
	shop_code,
	product_code,
	date_case,
	avg_3_sale,
	avg_3a_sale,
	avg_4_sale,
	avg_4a_sale,
	avg_5_sale,
	avg_5a_sale,
	avg_6_sale,
	avg_6a_sale,
	avg_7_sale,
	avg_7a_sale,
	avg_8_sale,
	avg_8a_sale,
	avg_9_sale,
	avg_9a_sale,
	avg_10_sale,
	avg_10a_sale,
	avg_11_sale,
	avg_11a_sale,
	avg_12_sale,
	avg_12a_sale,
	avg_13_sale,
	avg_13a_sale,
	avg_14_sale,
	avg_14a_sale,
	avg_15_sale,
	avg_15a_sale,
	avg_16_sale,
	avg_16a_sale,
	avg_17_sale,
	avg_17a_sale,
	avg_18_sale,
	avg_18a_sale,
	avg_19_sale,
	avg_19a_sale,
	avg_20_sale,
	avg_20a_sale,
	avg_21_sale,
	avg_21a_sale,
	avg_22_sale,
	avg_22a_sale,
	avg_23_sale,
	avg_23a_sale,
	avg_0_sale,
	avg_0a_sale,
	avg_1_sale,
	avg_1a_sale,
	avg_2_sale,
	avg_2a_sale,
	sale_date
)
select 
	t7.shop_code,
	t7.product_code,
	t7.date_case,
	avg(coalesce(t7.time_3_sale,0)) avg_3_sale,
	avg(coalesce(t7.time_3a_sale,0)) avg_3a_sale,
	avg(coalesce(t7.time_4_sale,0)) avg_4_sale,
	avg(coalesce(t7.time_4a_sale,0)) avg_4a_sale,
	avg(coalesce(t7.time_5_sale,0)) avg_5_sale,
	avg(coalesce(t7.time_5a_sale,0)) avg_5a_sale,
	avg(coalesce(t7.time_6_sale,0)) avg_6_sale,
	avg(coalesce(t7.time_6a_sale,0)) avg_6a_sale,
	avg(coalesce(t7.time_7_sale,0)) avg_7_sale,
	avg(coalesce(t7.time_7a_sale,0)) avg_7a_sale,
	avg(coalesce(t7.time_8_sale,0)) avg_8_sale,
	avg(coalesce(t7.time_8a_sale,0)) avg_8a_sale,
	avg(coalesce(t7.time_9_sale,0)) avg_9_sale,
	avg(coalesce(t7.time_9a_sale,0)) avg_9a_sale,
	avg(coalesce(t7.time_10_sale,0)) avg_10_sale,
	avg(coalesce(t7.time_10a_sale,0)) avg_10a_sale,
	avg(coalesce(t7.time_11_sale,0)) avg_11_sale,
	avg(coalesce(t7.time_11a_sale,0)) avg_11a_sale,
	avg(coalesce(t7.time_12_sale,0)) avg_12_sale,
	avg(coalesce(t7.time_12a_sale,0)) avg_12a_sale,
	avg(coalesce(t7.time_13_sale,0)) avg_13_sale,
	avg(coalesce(t7.time_13a_sale,0)) avg_13a_sale,
	avg(coalesce(t7.time_14_sale,0)) avg_14_sale,
	avg(coalesce(t7.time_14a_sale,0)) avg_14a_sale,
	avg(coalesce(t7.time_15_sale,0)) avg_15_sale,
	avg(coalesce(t7.time_15a_sale,0)) avg_15a_sale,
	avg(coalesce(t7.time_16_sale,0)) avg_16_sale,
	avg(coalesce(t7.time_16a_sale,0)) avg_16a_sale,
	avg(coalesce(t7.time_17_sale,0)) avg_17_sale,
	avg(coalesce(t7.time_17a_sale,0)) avg_17a_sale,
	avg(coalesce(t7.time_18_sale,0)) avg_18_sale,
	avg(coalesce(t7.time_18a_sale,0)) avg_18a_sale,
	avg(coalesce(t7.time_19_sale,0)) avg_19_sale,
	avg(coalesce(t7.time_19a_sale,0)) avg_19a_sale,
	avg(coalesce(t7.time_20_sale,0)) avg_20_sale,
	avg(coalesce(t7.time_20a_sale,0)) avg_20a_sale,
	avg(coalesce(t7.time_21_sale,0)) avg_21_sale,
	avg(coalesce(t7.time_21a_sale,0)) avg_21a_sale,
	avg(coalesce(t7.time_22_sale,0)) avg_22_sale,
	avg(coalesce(t7.time_22a_sale,0)) avg_22a_sale,
	avg(coalesce(t7.time_23_sale,0)) avg_23_sale,
	avg(coalesce(t7.time_23a_sale,0)) avg_23a_sale,
	avg(coalesce(t7.time_0_sale,0))  avg_0_sale,
	avg(coalesce(t7.time_0a_sale,0)) avg_0a_sale,
	avg(coalesce(t7.time_1_sale,0)) avg_1_sale,
	avg(coalesce(t7.time_1a_sale,0)) avg_1a_sale,
	avg(coalesce(t7.time_2_sale,0)) avg_2_sale,
	avg(coalesce(t7.time_2a_sale,0)) avg_2a_sale,
	$1 sale_date
from tmp7 t7
left join rst.rst_shop_date_case t8 
	on t7.shop_code=t8.shop_code 
	and t8."date"=$1
where t7.date_case=t8."case"
group by 
	t7.shop_code,
	t7.product_code,
	t7.date_case
;

drop table if exists tmp0;
drop table if exists tmp1;
drop table if exists tmp2;
drop table if exists tmp3;
drop table if exists tmp4;
drop table if exists tmp5;
drop table if exists tmp6;
drop table if exists tmp7;
drop table if exists tmp8;
drop table if exists tmp9;
drop table if exists tmp_a;
drop table if exists tmp_b;

	RETURN 1;
END

$function$
;

```