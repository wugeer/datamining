# sale_supply_avg
```
CREATE OR REPLACE FUNCTION rst.p_rst_sale_supply_avg(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

delete from rst.rst_sale_supply_case where sale_date=$1;
insert into rst.rst_sale_supply_case
(
shop_code,
product_code,
sale_date,
sale_min,
qty,
max_min,
date_case,
end_qty
)
with a as 
(
select shop_code,
product_code,
sale_date,
max(sale_min) max_min
from rst.rst_pos
group by shop_code,
				 product_code,
				 sale_date
)
select t1.shop_code,
t1.product_code,
t1.sale_date,
t1.sale_min,
t1.qty,
a.max_min,
t2."case" date_case,
t3.end_qty
from rst.rst_pos t1 
inner join rst.rst_product_onsale t4 on t1.shop_code=t4.shop_code and t1.product_code=t4.product_code
left join rst.rst_shop_date_case t2 on t1.shop_code=t2.shop_code and t1.sale_date=t2."date" 
left join rst.rst_inventory t3 on t1.shop_code=t3.shop_code and t1.product_code=t3.product_code and t1.sale_date=t3.check_date
left join a on t1.shop_code=a.shop_code and t1.product_code=a.product_code and t1.sale_date=a.sale_date
where t1.sale_date=$1
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
select shop_code,
product_code,
date_case,
sale_date,
max_min,
sum(case when sale_min>=180 then qty end) sum_3_sale,
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
from rst.rst_sale_supply_case 
where sale_date=$1
group by shop_code,
product_code,
date_case,
sale_date,
max_min,
end_qty
;

drop table if exists tmp3;
create temporary table tmp3 as
with b as 
(
select row_number() over(partition by shop_code,product_code,date_case order by sale_date desc) date_id,
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
where sale_date<=$1 and sale_date>=$1-'1 year'::interval
)
select shop_code,
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
from b 
where date_id<=100
;
--select * from tmp3 where shop_code='020895' and product_code='05050001'

drop table if exists tmp4;
create temporary table tmp4 as 
with c as
(
select PERCENTILE_CONT(0.25) within group(order by sum_3_sale) low,
			 PERCENTILE_CONT(0.75) within group(order by sum_3_sale) high, 
		   shop_code,
		   product_code,
		   date_case
from tmp3
group by shop_code,
		     product_code,
				 date_case
)
select shop_code,
product_code,
date_case,
low-1.5*(high-low) low,
high+1.5*(high-low) high
from c
;
--select * from tmp4 where shop_code='020895' and product_code='05050001'

drop table if exists tmp5;
create temporary table tmp5 as 
select t4.shop_code,
t4.product_code,
t4.date_case,
t4.sale_date,
t4.max_min,
t4.sum_3_sale,
t4.sum_3a_sale,
t4.sum_4_sale,
t4.sum_4a_sale,
t4.sum_5_sale,
t4.sum_5a_sale,
t4.sum_6_sale,
t4.sum_6a_sale,
t4.sum_7_sale,
t4.sum_7a_sale,
t4.sum_8_sale,
t4.sum_8a_sale,
t4.sum_9_sale,
t4.sum_9a_sale,
t4.sum_10_sale,
t4.sum_10a_sale,
t4.sum_11_sale,
t4.sum_11a_sale,
t4.sum_12_sale,
t4.sum_12a_sale,
t4.sum_13_sale,
t4.sum_13a_sale,
t4.sum_14_sale,
t4.sum_14a_sale,
t4.sum_15_sale,
t4.sum_15a_sale,
t4.sum_16_sale,
t4.sum_16a_sale,
t4.sum_17_sale,
t4.sum_17a_sale,
t4.sum_18_sale,
t4.sum_18a_sale,
t4.sum_19_sale,
t4.sum_19a_sale,
t4.sum_20_sale,
t4.sum_20a_sale,
t4.sum_21_sale,
t4.sum_21a_sale,
t4.sum_22_sale,
t4.sum_22a_sale,
t4.sum_23_sale,
t4.sum_23a_sale,
t4.sum_0_sale,
t4.sum_0a_sale,
t4.sum_1_sale,
t4.sum_1a_sale,
t4.sum_2_sale,
t4.sum_2a_sale,
end_qty
from tmp3 t4
left join tmp4 t5 on t4.shop_code=t5.shop_code and t4.product_code=t5.product_code and t4.date_case=t5.date_case
where t4.sum_3_sale>=t5.low and t4.sum_3_sale<=t5.high
;
--select * from tmp5 where shop_code='020895' and product_code='05050001'

drop table if exists tmp6;
create temporary table tmp6 as 
select shop_code,
product_code,
date_case,
sale_date,
max_min,
case when max_min>=180 and max_min<210 then sum_3_sale
	 else sum_3_sale-sum_3a_sale end time_3_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<210 then 0
	 when max_min>=210 and max_min<240 then sum_3a_sale
	 else sum_3a_sale-sum_4_sale end time_3a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<240 then 0
	 when max_min>=240 and max_min<270 then sum_4_sale
	 else sum_4_sale-sum_4a_sale end time_4_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<270 then 0
	 when max_min>=270 and max_min<300 then sum_4a_sale
	 else sum_4a_sale-sum_5_sale end time_4a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<300 then 0
	 when max_min>=300 and max_min<330 then sum_5_sale
	 else sum_5_sale-sum_5a_sale end time_5_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<330 then 0
	 when max_min>=330 and max_min<360 then sum_5a_sale
	 else sum_5a_sale-sum_6_sale end time_5a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<360 then 0
	 when max_min>=360 and max_min<390 then sum_6_sale
	 else sum_6_sale-sum_6a_sale end time_6_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<390 then 0
	 when max_min>=390 and max_min<420 then sum_6a_sale
	 else sum_6a_sale-sum_7_sale end time_6a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<420 then 0
	 when max_min>=420 and max_min<450 then sum_7_sale
	 else sum_7_sale-sum_7a_sale end time_7_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<450 then 0
	 when max_min>=450 and max_min<480 then sum_7a_sale
	 else sum_7a_sale-sum_8_sale end time_7a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<480 then 0
	 when max_min>=480 and max_min<510 then sum_8_sale
	 else sum_8_sale-sum_8a_sale end time_8_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<510 then 0
	 when max_min>=510 and max_min<540 then sum_8a_sale
	 else sum_8a_sale-sum_9_sale end time_8a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<540 then 0
	 when max_min>=540 and max_min<570 then sum_9_sale
	 else sum_9_sale-sum_9a_sale end time_9_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<570 then 0
	 when max_min>=570 and max_min<600 then sum_9a_sale
	 else sum_9a_sale-sum_10_sale end time_9a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<600 then 0
	 when max_min>=600 and max_min<630 then sum_10_sale
	 else sum_10_sale-sum_10a_sale end time_10_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<630 then 0
	 when max_min>=630 and max_min<660 then sum_10a_sale
	 else sum_10a_sale-sum_11_sale end time_10a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<660 then 0
	 when max_min>=660 and max_min<690 then sum_11_sale
	 else sum_11_sale-sum_11a_sale end time_11_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<690 then 0
	 when max_min>=690 and max_min<720 then sum_11a_sale
	 else sum_11a_sale-sum_12_sale end time_11a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<720 then 0
	 when max_min>=720 and max_min<750 then sum_12_sale
	 else sum_12_sale-sum_12a_sale end time_12_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<750 then 0
	 when max_min>=750 and max_min<780 then sum_12a_sale
	 else sum_12a_sale-sum_13_sale end time_12a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<780 then 0
	 when max_min>=780 and max_min<810 then sum_13_sale
	 else sum_13_sale-sum_13a_sale end time_13_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<810 then 0
	 when max_min>=810 and max_min<840 then sum_13a_sale
	 else sum_13a_sale-sum_14_sale end time_13a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<840 then 0
	 when max_min>=840 and max_min<870 then sum_14_sale
	 else sum_14_sale-sum_14a_sale end time_14_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<870 then 0
	 when max_min>=870 and max_min<900 then sum_14a_sale
	 else sum_14a_sale-sum_15_sale end time_14a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<900 then 0
	 when max_min>=900 and max_min<930 then sum_15_sale
	 else sum_15_sale-sum_15a_sale end time_15_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<930 then 0
	 when max_min>=930 and max_min<960 then sum_15a_sale
	 else sum_15a_sale-sum_16_sale end time_15a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<960 then 0
	 when max_min>=960 and max_min<990 then sum_16_sale
	 else sum_16_sale-sum_16a_sale end time_16_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<990 then 0
	 when max_min>=990 and max_min<1020 then sum_16a_sale
	 else sum_16a_sale-sum_17_sale end time_16a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1020 then 0
	 when max_min>=1020 and max_min<1050 then sum_17_sale
	 else sum_17_sale-sum_17a_sale end time_17_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1050 then 0
	 when max_min>=1050 and max_min<1080 then sum_17a_sale
	 else sum_17a_sale-sum_18_sale end time_17a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1080 then 0
	 when max_min>=1080 and max_min<1110 then sum_18_sale
	 else sum_18_sale-sum_18a_sale end time_18_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1110 then 0
	 when max_min>=1110 and max_min<1140 then sum_18a_sale
	 else sum_18a_sale-sum_19_sale end time_18a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1140 then 0
	 when max_min>=1140 and max_min<1170 then sum_19_sale
	 else sum_19_sale-sum_19a_sale end time_19_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1170 then 0
	 when max_min>=1170 and max_min<1200 then sum_19a_sale
	 else sum_19a_sale-sum_20_sale end time_19a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1200 then 0
	 when max_min>=1200 and max_min<1230 then sum_20_sale
	 else sum_20_sale-sum_20a_sale end time_20_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1230 then 0
	 when max_min>=1230 and max_min<1260 then sum_20a_sale
	 else sum_20a_sale-sum_21_sale end time_20a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1260 then 0
	 when max_min>=1260 and max_min<1290 then sum_21_sale
	 else sum_21_sale-sum_21a_sale end time_21_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1290 then 0
	 when max_min>=1290 and max_min<1320 then sum_21a_sale
	 else sum_21a_sale-sum_22_sale end time_21a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1320 then 0
	 when max_min>=1320 and max_min<1350 then sum_22_sale
	 else sum_22_sale-sum_22a_sale end time_22_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1350 then 0
	 when max_min>=1350 and max_min<1380 then sum_22a_sale
	 else sum_22a_sale-sum_23_sale end time_22a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1380 then 0
	 when max_min>=1380 and max_min<1410 then sum_23_sale
	 else sum_23_sale-sum_23a_sale end time_23_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1410 then 0
	 when max_min>=1410 and max_min<1440 then sum_23a_sale
	 else sum_23a_sale-sum_0_sale end time_23a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1440 then 0
	 when max_min>=1440 and max_min<1470 then sum_0_sale
	 else sum_0_sale-sum_0a_sale end time_0_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1470 then 0
	 when max_min>=1470 and max_min<1500 then sum_0a_sale
	 else sum_0a_sale-sum_1_sale end time_0a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1500 then 0
	 when max_min>=1500 and max_min<1530 then sum_1_sale
	 else sum_1_sale-sum_1a_sale end time_1_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1530 then 0
	 when max_min>=1530 and max_min<1560 then sum_1a_sale
	 else sum_1a_sale-sum_2_sale end time_1a_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1560 then 0
	 when max_min>=1560 and max_min<1590 then sum_2_sale
	 else sum_2_sale-sum_2a_sale end time_2_sale,
case when (end_qty<>0 and end_qty is not null) and max_min<1590 then 0
	 else sum_2a_sale end time_2a_sale,
end_qty
from tmp5
;
--select * from tmp6 where shop_code='020895' and product_code='05050001'

drop table if exists tmp7;
create temporary table tmp7 as 
with e as 
(
select row_number() over(partition by shop_code,product_code,date_case order by sale_date desc) date_id,
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
)
select shop_code,
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
from e 
where date_id<=30
;
--select * from tmp7 where shop_code='020895' and product_code='05050001';


delete from rst.rst_sale_supply_avg where sale_date=$1;
insert into rst.rst_sale_supply_avg
select shop_code,
product_code,
date_case,
coalesce(avg(time_3_sale),0) avg_3_sale,
coalesce(avg(time_3a_sale),0) avg_3a_sale,
coalesce(avg(time_4_sale),0) avg_4_sale,
coalesce(avg(time_4a_sale),0) avg_4a_sale,
coalesce(avg(time_5_sale),0) avg_5_sale,
coalesce(avg(time_5a_sale),0) avg_5a_sale,
coalesce(avg(time_6_sale),0) avg_6_sale,
coalesce(avg(time_6a_sale),0) avg_6a_sale,
coalesce(avg(time_7_sale),0) avg_7_sale,
coalesce(avg(time_7a_sale),0) avg_7a_sale,
coalesce(avg(time_8_sale),0) avg_8_sale,
coalesce(avg(time_8a_sale),0) avg_8a_sale,
coalesce(avg(time_9_sale),0) avg_9_sale,
coalesce(avg(time_9a_sale),0) avg_9a_sale,
coalesce(avg(time_10_sale),0) avg_10_sale,
coalesce(avg(time_10a_sale),0) avg_10a_sale,
coalesce(avg(time_11_sale),0) avg_11_sale,
coalesce(avg(time_11a_sale),0) avg_11a_sale,
coalesce(avg(time_12_sale),0) avg_12_sale,
coalesce(avg(time_12a_sale),0) avg_12a_sale,
coalesce(avg(time_13_sale),0) avg_13_sale,
coalesce(avg(time_13a_sale),0) avg_13a_sale,
coalesce(avg(time_14_sale),0) avg_14_sale,
coalesce(avg(time_14a_sale),0) avg_14a_sale,
coalesce(avg(time_15_sale),0) avg_15_sale,
coalesce(avg(time_15a_sale),0) avg_15a_sale,
coalesce(avg(time_16_sale),0) avg_16_sale,
coalesce(avg(time_16a_sale),0) avg_16a_sale,
coalesce(avg(time_17_sale),0) avg_17_sale,
coalesce(avg(time_17a_sale),0) avg_17a_sale,
coalesce(avg(time_18_sale),0) avg_18_sale,
coalesce(avg(time_18a_sale),0) avg_18a_sale,
coalesce(avg(time_19_sale),0) avg_19_sale,
coalesce(avg(time_19a_sale),0) avg_19a_sale,
coalesce(avg(time_20_sale),0) avg_20_sale,
coalesce(avg(time_20a_sale),0) avg_20a_sale,
coalesce(avg(time_21_sale),0) avg_21_sale,
coalesce(avg(time_21a_sale),0) avg_21a_sale,
coalesce(avg(time_22_sale),0) avg_22_sale,
coalesce(avg(time_22a_sale),0) avg_22a_sale,
coalesce(avg(time_23_sale),0) avg_23_sale,
coalesce(avg(time_23a_sale),0) avg_23a_sale,
coalesce(avg(time_0_sale),0)  avg_0_sale,
coalesce(avg(time_0a_sale),0) avg_0a_sale,
coalesce(avg(time_1_sale),0) avg_1_sale,
coalesce(avg(time_1a_sale),0) avg_1a_sale,
coalesce(avg(time_2_sale),0) avg_2_sale,
coalesce(avg(time_2a_sale),0) avg_2a_sale,
$1 sale_date
from tmp7
group by shop_code,
product_code,
date_case
;
--select * from tmp8 where shop_code='020895' and product_code='05050001';

drop table if exists tmp1;
drop table if exists tmp2;
drop table if exists tmp3;
drop table if exists tmp4;
drop table if exists tmp5;
drop table if exists tmp6;
drop table if exists tmp7;

	RETURN 1;
END

$function$
;

```