# rate_of_shorts
```
CREATE OR REPLACE FUNCTION rst.p_rst_rate_of_shorts(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--计算当天有库存的商品（含进货）的期初库存
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t1.shop_code,
	t1.product_code,
	t1.check_date,
	coalesce(t1.begin_qty,t2.inferential_end_qty,0)+coalesce(t1.in_qty,0) begin_qty
from rst.rst_inventory t1
left join rst.rst_inventory t2 
	on t1.shop_code=t2.shop_code 
	and t1.product_code=t2.product_code 
	and t1.check_date=(t2.check_date+'1 day'::interval)::date
where t1.check_date=$1
	and coalesce(t1.begin_qty,t2.inferential_end_qty,0)+coalesce(t1.in_qty,0)>0
;

--计算当天有库存的商品数量
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	shop_code,
	check_date,
	count(*) stock_qty
from tmp1 
group by 
	shop_code,
	check_date
;

--计算累计营业额和90%营业额的值
drop table if exists tmp3;
create temporary table tmp3 as 
select 
	shop_code,
	sale_date,
	time_3_turnover sum_3_turnover,
	time_3_turnover+time_4_turnover sum_4_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover sum_5_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover sum_6_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover sum_7_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover sum_8_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover sum_9_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover sum_10_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover sum_11_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover sum_12_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover sum_13_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover sum_14_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover sum_15_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover sum_16_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover+time_17_turnover sum_17_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover+time_17_turnover+time_18_turnover sum_18_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover+time_17_turnover+time_18_turnover+time_19_turnover sum_19_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover+time_17_turnover+time_18_turnover+time_19_turnover+time_20_turnover sum_20_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover+time_17_turnover+time_18_turnover+time_19_turnover+time_20_turnover+time_21_turnover sum_21_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover+time_17_turnover+time_18_turnover+time_19_turnover+time_20_turnover+time_21_turnover+time_22_turnover sum_22_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover+time_17_turnover+time_18_turnover+time_19_turnover+time_20_turnover+time_21_turnover+time_22_turnover+time_23_turnover sum_23_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover+time_17_turnover+time_18_turnover+time_19_turnover+time_20_turnover+time_21_turnover+time_22_turnover+time_23_turnover+time_0_turnover sum_0_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover+time_17_turnover+time_18_turnover+time_19_turnover+time_20_turnover+time_21_turnover+time_22_turnover+time_23_turnover+time_0_turnover+time_1_turnover sum_1_turnover,
	time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover+time_17_turnover+time_18_turnover+time_19_turnover+time_20_turnover+time_21_turnover+time_22_turnover+time_23_turnover+time_0_turnover+time_1_turnover+time_2_turnover sum_2_turnover,
	(time_3_turnover+time_4_turnover+time_5_turnover+time_6_turnover+time_7_turnover+time_8_turnover+time_9_turnover+time_10_turnover+time_11_turnover+time_12_turnover+time_13_turnover+time_14_turnover+time_15_turnover+time_16_turnover+time_17_turnover+time_18_turnover+time_19_turnover+time_20_turnover+time_21_turnover+time_22_turnover+time_23_turnover+time_0_turnover+time_1_turnover+time_2_turnover)*0.9 percent_90_turnover
from rst.rst_turnover_feat_sum
where sale_date=$1
;

--找出营业额超过90%的时间点
drop table if exists tmp4;
create temporary table tmp4 as 
select 
	shop_code,
	sale_date,
	'1'::varchar is_turnover,
	case when sum_3_turnover>=percent_90_turnover then '4:00'::time
		 when sum_3_turnover<percent_90_turnover and sum_4_turnover>=percent_90_turnover then '5:00'::time
		 when sum_4_turnover<percent_90_turnover and sum_5_turnover>=percent_90_turnover then '6:00'::time
		 when sum_5_turnover<percent_90_turnover and sum_6_turnover>=percent_90_turnover then '7:00'::time
		 when sum_6_turnover<percent_90_turnover and sum_7_turnover>=percent_90_turnover then '8:00'::time
		 when sum_7_turnover<percent_90_turnover and sum_8_turnover>=percent_90_turnover then '9:00'::time
		 when sum_8_turnover<percent_90_turnover and sum_9_turnover>=percent_90_turnover then '10:00'::time
		 when sum_9_turnover<percent_90_turnover and sum_10_turnover>=percent_90_turnover then '11:00'::time
		 when sum_10_turnover<percent_90_turnover and sum_11_turnover>=percent_90_turnover then '12:00'::time
		 when sum_11_turnover<percent_90_turnover and sum_12_turnover>=percent_90_turnover then '13:00'::time
		 when sum_12_turnover<percent_90_turnover and sum_13_turnover>=percent_90_turnover then '14:00'::time
		 when sum_13_turnover<percent_90_turnover and sum_14_turnover>=percent_90_turnover then '15:00'::time
		 when sum_14_turnover<percent_90_turnover and sum_15_turnover>=percent_90_turnover then '16:00'::time
		 when sum_15_turnover<percent_90_turnover and sum_16_turnover>=percent_90_turnover then '17:00'::time
		 when sum_16_turnover<percent_90_turnover and sum_17_turnover>=percent_90_turnover then '18:00'::time
		 when sum_17_turnover<percent_90_turnover and sum_18_turnover>=percent_90_turnover then '19:00'::time
		 when sum_18_turnover<percent_90_turnover and sum_19_turnover>=percent_90_turnover then '20:00'::time
		 when sum_19_turnover<percent_90_turnover and sum_20_turnover>=percent_90_turnover then '21:00'::time
		 when sum_20_turnover<percent_90_turnover and sum_21_turnover>=percent_90_turnover then '22:00'::time
		 when sum_21_turnover<percent_90_turnover and sum_22_turnover>=percent_90_turnover then '23:00'::time
		 when sum_22_turnover<percent_90_turnover and sum_23_turnover>=percent_90_turnover then '00:00'::time
		 when sum_23_turnover<percent_90_turnover and sum_0_turnover>=percent_90_turnover then '1:00'::time
		 when sum_0_turnover<percent_90_turnover and sum_1_turnover>=percent_90_turnover then '2:00'::time
		 when sum_1_turnover<percent_90_turnover and sum_2_turnover>=percent_90_turnover then '3:00'::time
	end cut_off_time
from tmp3
;
--select * from tmp4

--合并固定时间点和90%营业额时间点
drop table if exists tmp5;
create temporary table tmp5 as 
(
	select 
		t3.shop_code,
		t4.cut_off_time,
		'0'::varchar is_turnover
	from tmp2 t3
	cross join (select generate_series($1+'17:00'::time,$1+'19:00'::time,'1 hour') cut_off_time) t4
)
union 
(
	select 
		shop_code,
		sale_date+cut_off_time cut_off_time,
		is_turnover
	from tmp4
)
;
--select * from tmp5

--转化小时为分钟，以求累计销量
drop table if exists tmp6;
create temporary table tmp6 as 
select 
	shop_code,
	cut_off_time,
	is_turnover,
	case when cut_off_time::time='4:00'::time then 240
		 when cut_off_time::time='5:00'::time then 300
		 when cut_off_time::time='6:00'::time then 360
		 when cut_off_time::time='7:00'::time then 420
		 when cut_off_time::time='8:00'::time then 480
		 when cut_off_time::time='9:00'::time then 540
		 when cut_off_time::time='10:00'::time then 600
		 when cut_off_time::time='11:00'::time then 660
		 when cut_off_time::time='12:00'::time then 720
		 when cut_off_time::time='13:00'::time then 780
		 when cut_off_time::time='14:00'::time then 840
		 when cut_off_time::time='15:00'::time then 900
		 when cut_off_time::time='16:00'::time then 960
		 when cut_off_time::time='17:00'::time then 1020
		 when cut_off_time::time='18:00'::time then 1080
		 when cut_off_time::time='19:00'::time then 1140
		 when cut_off_time::time='20:00'::time then 1200
		 when cut_off_time::time='21:00'::time then 1260
		 when cut_off_time::time='22:00'::time then 1320
		 when cut_off_time::time='23:00'::time then 1380
		 when cut_off_time::time='00:00'::time then 1440
		 when cut_off_time::time='1:00'::time then 1500
		 when cut_off_time::time='2:00'::time then 1560
		 when cut_off_time::time='3:00'::time then 1620
	end cut_off_min
from tmp5
;
--select * from tmp6

--得到门店有库存的商品
drop table if exists tmp7;
create temporary table tmp7 as 
select 
	t5.shop_code,
	t6.product_code,
	t5.cut_off_time,
	t5.is_turnover,
	t5.cut_off_min
from tmp6 t5
left join tmp1 t6 
	on t5.shop_code=t6.shop_code 
	and t5.cut_off_time::date=t6.check_date
;

--计算各时间点累计销量
drop table if exists tmp8;
create temporary table tmp8 as 
select 
	t7.shop_code,
	t7.product_code,
	t7.cut_off_time,
	t7.is_turnover,
	t7.cut_off_min,
	sum(coalesce(t8.qty,0)) sum_qty
from tmp7 t7
left join rst.rst_pos t8 
	on t7.shop_code=t8.shop_code
	and t7.product_code=t8.product_code
	and t7.cut_off_time::date=t8.sale_date
	and t7.cut_off_min>t8.sale_min
group by 
	t7.shop_code,
	t7.product_code,
	t7.cut_off_time,
	t7.is_turnover,
	t7.cut_off_min
;
--select * from tmp8 order by shop_code,product_code,cut_off_time

--判断每个门店的断货商品
drop table if exists tmp9;
create temporary table tmp9 as 
select 
	t9.shop_code,
	t9.product_code,
	t9.cut_off_time,
	t9.is_turnover
from tmp8 t9
left join tmp1 t10 
	on t9.shop_code=t10.shop_code
	and t9.product_code=t10.product_code
	and t9.cut_off_time::date=t10.check_date
where t9.sum_qty>=t10.begin_qty
;
--select * from tmp9 order by shop_code,product_code,cut_off_time

--计算断货商品个数
drop table if exists tmp10;
create temporary table tmp10 as 
select 
	shop_code,
	cut_off_time,
	is_turnover,
	count(product_code) out_of_stock_qty
from tmp9 
group by 
	shop_code,
	cut_off_time,
	is_turnover
;
--select * from tmp10

--计算断货率
drop table if exists tmp11;
create temporary table tmp11 as 
select 
	t11.shop_code,
	t11.cut_off_time,
	t11.is_turnover,
	t11.out_of_stock_qty/t12.stock_qty::float rate
from tmp10 t11
left join tmp2 t12 
	on t11.shop_code=t12.shop_code
	and t11.cut_off_time::date=t12.check_date 
;
--select * from tmp11

--计算断货率
delete from rst.rst_rate_of_shorts where cut_off_time::date=$1;
insert into rst.rst_rate_of_shorts
(
	shop_code,
	product_code,
	cut_off_time,
	is_turnover,
	rate
) 
select 
	t13.shop_code,
	t13.product_code,
	t13.cut_off_time,
	t13.is_turnover,
	t14.rate
from tmp9 t13
left join tmp11 t14 
	on t13.shop_code=t14.shop_code
	and t13.cut_off_time=t14.cut_off_time
	and t13.is_turnover=t14.is_turnover
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

	RETURN 1;
END

$function$
;

```