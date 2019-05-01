# sale_suplly_copy
```
CREATE OR REPLACE FUNCTION rst.p_rst_sale_supply_copy(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	--计算日末库寸为end_qty，sale_supply_avg为本周一的销补
DECLARE
v_count int:=1;--sql影响条数
BEGIN

--取当天inventory记录
drop table if exists tmp_a;
create temp table tmp_a as 
select shop_code,
product_code,
check_date,
begin_qty,
in_qty,
group_buying_qty,
sale_qty,
sale_supply_qty,
loss_qty,
end_qty,
sale_total_qty,
inferential_end_qty
from rst.rst_inventory
where check_date=$1
;

--取有销售记录且断货的商品
drop table if exists tmp1;
create temp table tmp1 as 
select shop_code,
		product_code,
		date_trunc('week',sale_date) sale_date,
		--sale_date,
		date_case,
		max_min
	from rst.rst_sale_supply_sum 
	where (end_qty=0 or end_qty is null) 
		--and sale_date='2017-07-06'::date
		and sale_date=$1
;

--计算销补
drop table if exists tmp9;
create temporary table tmp9 as 
select f.shop_code,
f.product_code,
f.date_case,
case when f.max_min>=180 and f.max_min<210 then t5.avg_3a_sale+t5.avg_4_sale+t5.avg_4a_sale+t5.avg_5_sale+t5.avg_5a_sale+t5.avg_6_sale+t5.avg_6a_sale+t5.avg_7_sale+t5.avg_7a_sale+t5.avg_8_sale+t5.avg_8a_sale+t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=210 and f.max_min<240 then t5.avg_4_sale+t5.avg_4a_sale+t5.avg_5_sale+t5.avg_5a_sale+t5.avg_6_sale+t5.avg_6a_sale+t5.avg_7_sale+t5.avg_7a_sale+t5.avg_8_sale+t5.avg_8a_sale+t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=240 and f.max_min<270 then t5.avg_4a_sale+t5.avg_5_sale+t5.avg_5a_sale+t5.avg_6_sale+t5.avg_6a_sale+t5.avg_7_sale+t5.avg_7a_sale+t5.avg_8_sale+t5.avg_8a_sale+t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=270 and f.max_min<300 then t5.avg_5_sale+t5.avg_5a_sale+t5.avg_6_sale+t5.avg_6a_sale+t5.avg_7_sale+t5.avg_7a_sale+t5.avg_8_sale+t5.avg_8a_sale+t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=300 and f.max_min<330 then t5.avg_5a_sale+t5.avg_6_sale+t5.avg_6a_sale+t5.avg_7_sale+t5.avg_7a_sale+t5.avg_8_sale+t5.avg_8a_sale+t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=330 and f.max_min<360 then t5.avg_6_sale+t5.avg_6a_sale+t5.avg_7_sale+t5.avg_7a_sale+t5.avg_8_sale+t5.avg_8a_sale+t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=360 and f.max_min<390 then t5.avg_6a_sale+t5.avg_7_sale+t5.avg_7a_sale+t5.avg_8_sale+t5.avg_8a_sale+t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=390 and f.max_min<420 then t5.avg_7_sale+t5.avg_7a_sale+t5.avg_8_sale+t5.avg_8a_sale+t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=420 and f.max_min<450 then t5.avg_7a_sale+t5.avg_8_sale+t5.avg_8a_sale+t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=450 and f.max_min<480 then t5.avg_8_sale+t5.avg_8a_sale+t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=480 and f.max_min<510 then t5.avg_8a_sale+t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=510 and f.max_min<540 then t5.avg_9_sale+t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=540 and f.max_min<570 then t5.avg_9a_sale+t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=570 and f.max_min<600 then t5.avg_10_sale+t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=600 and f.max_min<630 then t5.avg_10a_sale+t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=630 and f.max_min<660 then t5.avg_11_sale+t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=660 and f.max_min<690 then t5.avg_11a_sale+t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=690 and f.max_min<720 then t5.avg_12_sale+t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=720 and f.max_min<750 then t5.avg_12a_sale+t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=750 and f.max_min<780 then t5.avg_13_sale+t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=780 and f.max_min<810 then t5.avg_13a_sale+t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=810 and f.max_min<840 then t5.avg_14_sale+t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=840 and f.max_min<870 then t5.avg_14a_sale+t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=870 and f.max_min<900 then t5.avg_15_sale+t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=900 and f.max_min<930 then t5.avg_15a_sale+t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=930 and f.max_min<960 then t5.avg_16_sale+t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=960 and f.max_min<990 then t5.avg_16a_sale+t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=990 and f.max_min<1020 then t5.avg_17_sale+t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1020 and f.max_min<1050 then t5.avg_17a_sale+t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1050 and f.max_min<1080 then t5.avg_18_sale+t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1080 and f.max_min<1110 then t5.avg_18a_sale+t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1110 and f.max_min<1140 then t5.avg_19_sale+t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1140 and f.max_min<1170 then t5.avg_19a_sale+t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1170 and f.max_min<1200 then t5.avg_20_sale+t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1200 and f.max_min<1230 then t5.avg_20a_sale+t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1230 and f.max_min<1260 then t5.avg_21_sale+t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1260 and f.max_min<1290 then t5.avg_21a_sale+t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1290 and f.max_min<1320 then t5.avg_22_sale+t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1320 and f.max_min<1350 then t5.avg_22a_sale+t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1350 and f.max_min<1380 then t5.avg_23_sale+t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1380 and f.max_min<1410 then t5.avg_23a_sale+t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1410 and f.max_min<1440 then t5.avg_0_sale+t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1440 and f.max_min<1470 then t5.avg_0a_sale+t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1470 and f.max_min<1500 then t5.avg_1_sale+t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1500 and f.max_min<1530 then t5.avg_1a_sale+t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1530 and f.max_min<1560 then t5.avg_2_sale+t5.avg_2a_sale
     when f.max_min>=1560 and f.max_min<1590 then t5.avg_2a_sale
	 when f.max_min>=1590 then 0 end sale_supply_qty
from tmp1 f 
left join rst.rst_sale_supply_avg_copy t5 on f.shop_code=t5.shop_code 
	and f.product_code=t5.product_code and f.date_case=t5.date_case 
	and f.sale_date=t5.sale_date
;
GET DIaGNOSTICS v_count = ROW_COUNT;
raise notice 'tmp9: %', v_count;

--取所有断货商品
drop table if exists tmp_b;
create temp table tmp_b as 
select t8.shop_code,
t8.product_code,
t8.check_date,
t9."case"
from tmp_a t8
left join rst.rst_shop_date_case t9 on t8.shop_code=t9.shop_code and t8.check_date=t9."date"
where (t8.end_qty is null or t8.end_qty=0) and t8.check_date=$1 
;

--计算销补
drop table if exists tmp10;
create temporary table tmp10 as 
select a.shop_code,
a.product_code,
a.check_date,
t10.sale_supply_qty
from tmp_b a
left join 
(
select shop_code,
product_code,
sale_date,
date_case,
(avg_3a_sale+avg_4_sale+avg_4a_sale+avg_5_sale+avg_5a_sale+avg_6_sale+avg_6a_sale+avg_7_sale+avg_7a_sale+avg_8_sale+avg_8a_sale+avg_9_sale+avg_9a_sale+avg_10_sale+avg_10a_sale+avg_11_sale+avg_11a_sale+avg_12_sale+avg_12a_sale+avg_13_sale+avg_13a_sale+avg_14_sale+avg_14a_sale+avg_15_sale+avg_15a_sale+avg_16_sale+avg_16a_sale+avg_17_sale+avg_17a_sale+avg_18_sale+avg_18a_sale+avg_19_sale+avg_19a_sale+avg_20_sale+avg_20a_sale+avg_21_sale+avg_21a_sale+avg_22_sale+avg_22a_sale+avg_23_sale+avg_23a_sale+avg_0_sale+avg_0a_sale+avg_1_sale+avg_1a_sale+avg_2_sale+avg_2a_sale) sale_supply_qty
from rst.rst_sale_supply_avg_copy
) t10 on a.shop_code=t10.shop_code 
and a.product_code=t10.product_code 
and a."case"=t10.date_case 
and date_trunc('week',a.check_date)=t10.sale_date
--and t10.sale_date=$1
;
/*
drop table if exists tmp10; 
create temp table tmp10 as 
select t8.shop_code,
t8.product_code,
t8.check_date,
(t10.avg_3a_sale+t10.avg_4_sale+t10.avg_4a_sale+t10.avg_5_sale+t10.avg_5a_sale+t10.avg_6_sale+t10.avg_6a_sale+t10.avg_7_sale+t10.avg_7a_sale+t10.avg_8_sale+t10.avg_8a_sale+t10.avg_9_sale+t10.avg_9a_sale+t10.avg_10_sale+t10.avg_10a_sale+t10.avg_11_sale+t10.avg_11a_sale+t10.avg_12_sale+t10.avg_12a_sale+t10.avg_13_sale+t10.avg_13a_sale+t10.avg_14_sale+t10.avg_14a_sale+t10.avg_15_sale+t10.avg_15a_sale+t10.avg_16_sale+t10.avg_16a_sale+t10.avg_17_sale+t10.avg_17a_sale+t10.avg_18_sale+t10.avg_18a_sale+t10.avg_19_sale+t10.avg_19a_sale+t10.avg_20_sale+t10.avg_20a_sale+t10.avg_21_sale+t10.avg_21a_sale+t10.avg_22_sale+t10.avg_22a_sale+t10.avg_23_sale+t10.avg_23a_sale+t10.avg_0_sale+t10.avg_0a_sale+t10.avg_1_sale+t10.avg_1a_sale+t10.avg_2_sale+t10.avg_2a_sale) sale_supply_qty
from rst.rst_inventory t8
left join rst.rst_shop_date_case t9 on t8.shop_code=t9.shop_code and t8.check_date=t9."date"
left join rst.rst_sale_supply_avg_copy t10 on t8.shop_code=t10.shop_code and t8.product_code=t10.product_code and t9."case"=t10.date_case and date_trunc('week',t8.check_date)=t10.sale_date
--left join rst.rst_sale_supply_avg_copy t10 on t8.shop_code=t10.shop_code and t8.product_code=t10.product_code and t9."case"=t10.date_case and t10.sale_date='2017-07-06'::date
where (t8.end_qty is null or t8.end_qty=0) 
	--and t8.check_date='2017-07-06'::date 
	and t8.check_date=$1
;
*/


drop table if exists tmp11;
create temp table tmp11 as 
select a.shop_code,
a.product_code,
a.check_date,
a.begin_qty,
a.in_qty,
a.group_buying_qty,
a.sale_qty,
case when b.sale_supply_qty is not null then b.sale_supply_qty
		 else c.sale_supply_qty end sale_supply_qty,
a.loss_qty,
a.end_qty,
case when b.sale_supply_qty is not null then coalesce(sale_qty,0)+coalesce(b.sale_supply_qty,0)
		 else coalesce(sale_qty,0)+coalesce(c.sale_supply_qty,0) end sale_total_qty,
a.inferential_end_qty
from tmp_a a
left join tmp9 b on a.shop_code=b.shop_code and a.product_code=b.product_code 
left join tmp10 c on a.shop_code=c.shop_code and a.product_code=c.product_code
where a.check_date=$1
;

GET DIaGNOSTICS v_count = ROW_COUNT;
raise notice 'tmp11: %', v_count;

delete from rst.rst_inventory 
where check_date=$1;
--select count(*) from rst.rst_inventory where check_date='2017-07-06'::date and sale_supply_qty is not null--$1;
insert into rst.rst_inventory
(
shop_code,
product_code,
check_date,
begin_qty,
in_qty,
group_buying_qty,
sale_qty,
sale_supply_qty,
loss_qty,
end_qty,
sale_total_qty,
inferential_end_qty
)
select shop_code,
product_code,
check_date,
begin_qty,
in_qty,
group_buying_qty,
sale_qty,
sale_supply_qty,
loss_qty,
end_qty,
sale_total_qty,
inferential_end_qty
from tmp11
;

GET DIaGNOSTICS v_count = ROW_COUNT;
raise notice 'insert: %', v_count;
drop table if exists tmp_a;
drop table if exists tmp_b;
drop table if exists tmp1;
drop table if exists tmp9;
drop table if exists tmp10;
drop table if exists tmp11;

	RETURN 1;
END

$function$
;

```