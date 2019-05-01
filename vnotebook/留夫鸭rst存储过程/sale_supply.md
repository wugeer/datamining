# sale_supply
```
CREATE OR REPLACE FUNCTION rst.p_rst_sale_supply(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	DECLARE
v_count int:=1;--sql影响条数
BEGIN

--取当天inventory记录
drop table if exists tmp_a;
create temp table tmp_a as 
select 
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
	inferential_end_qty,
	taste_give_qty
from rst.rst_inventory
where check_date=$1
;

--取有销售记录且断货的商品
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t1.shop_code,
	t1.product_code,
	--date_trunc('week',t1.sale_date) sale_date,
	t1.sale_date,
	t1.date_case,
	t1.max_min
from rst.rst_sale_supply_sum t1
left join rst.rst_inventory t2 
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
	and t1.sale_date=t2.check_date
where (t1.end_qty=0 or t1.end_qty is null) 
	and t2.loss_qty is null
		--and t1.sale_date='2017-07-06'::date
	and t1.sale_date=$1
;

--计算销补
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	t1.shop_code,
	t1.product_code,
	t1.sale_date,
	t1.date_case,
	case when t1.max_min>=180 and t1.max_min<210 then t2.avg_3a_sale+t2.avg_4_sale+t2.avg_4a_sale+t2.avg_5_sale+t2.avg_5a_sale+t2.avg_6_sale+t2.avg_6a_sale+t2.avg_7_sale+t2.avg_7a_sale+t2.avg_8_sale+t2.avg_8a_sale+t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=210 and t1.max_min<240 then t2.avg_4_sale+t2.avg_4a_sale+t2.avg_5_sale+t2.avg_5a_sale+t2.avg_6_sale+t2.avg_6a_sale+t2.avg_7_sale+t2.avg_7a_sale+t2.avg_8_sale+t2.avg_8a_sale+t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=240 and t1.max_min<270 then t2.avg_4a_sale+t2.avg_5_sale+t2.avg_5a_sale+t2.avg_6_sale+t2.avg_6a_sale+t2.avg_7_sale+t2.avg_7a_sale+t2.avg_8_sale+t2.avg_8a_sale+t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=270 and t1.max_min<300 then t2.avg_5_sale+t2.avg_5a_sale+t2.avg_6_sale+t2.avg_6a_sale+t2.avg_7_sale+t2.avg_7a_sale+t2.avg_8_sale+t2.avg_8a_sale+t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=300 and t1.max_min<330 then t2.avg_5a_sale+t2.avg_6_sale+t2.avg_6a_sale+t2.avg_7_sale+t2.avg_7a_sale+t2.avg_8_sale+t2.avg_8a_sale+t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=330 and t1.max_min<360 then t2.avg_6_sale+t2.avg_6a_sale+t2.avg_7_sale+t2.avg_7a_sale+t2.avg_8_sale+t2.avg_8a_sale+t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=360 and t1.max_min<390 then t2.avg_6a_sale+t2.avg_7_sale+t2.avg_7a_sale+t2.avg_8_sale+t2.avg_8a_sale+t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=390 and t1.max_min<420 then t2.avg_7_sale+t2.avg_7a_sale+t2.avg_8_sale+t2.avg_8a_sale+t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=420 and t1.max_min<450 then t2.avg_7a_sale+t2.avg_8_sale+t2.avg_8a_sale+t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=450 and t1.max_min<480 then t2.avg_8_sale+t2.avg_8a_sale+t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=480 and t1.max_min<510 then t2.avg_8a_sale+t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=510 and t1.max_min<540 then t2.avg_9_sale+t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=540 and t1.max_min<570 then t2.avg_9a_sale+t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=570 and t1.max_min<600 then t2.avg_10_sale+t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=600 and t1.max_min<630 then t2.avg_10a_sale+t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=630 and t1.max_min<660 then t2.avg_11_sale+t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=660 and t1.max_min<690 then t2.avg_11a_sale+t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=690 and t1.max_min<720 then t2.avg_12_sale+t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=720 and t1.max_min<750 then t2.avg_12a_sale+t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=750 and t1.max_min<780 then t2.avg_13_sale+t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=780 and t1.max_min<810 then t2.avg_13a_sale+t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=810 and t1.max_min<840 then t2.avg_14_sale+t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=840 and t1.max_min<870 then t2.avg_14a_sale+t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=870 and t1.max_min<900 then t2.avg_15_sale+t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=900 and t1.max_min<930 then t2.avg_15a_sale+t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=930 and t1.max_min<960 then t2.avg_16_sale+t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=960 and t1.max_min<990 then t2.avg_16a_sale+t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=990 and t1.max_min<1020 then t2.avg_17_sale+t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1020 and t1.max_min<1050 then t2.avg_17a_sale+t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1050 and t1.max_min<1080 then t2.avg_18_sale+t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1080 and t1.max_min<1110 then t2.avg_18a_sale+t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1110 and t1.max_min<1140 then t2.avg_19_sale+t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1140 and t1.max_min<1170 then t2.avg_19a_sale+t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1170 and t1.max_min<1200 then t2.avg_20_sale+t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1200 and t1.max_min<1230 then t2.avg_20a_sale+t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1230 and t1.max_min<1260 then t2.avg_21_sale+t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1260 and t1.max_min<1290 then t2.avg_21a_sale+t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1290 and t1.max_min<1320 then t2.avg_22_sale+t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1320 and t1.max_min<1350 then t2.avg_22a_sale+t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1350 and t1.max_min<1380 then t2.avg_23_sale+t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1380 and t1.max_min<1410 then t2.avg_23a_sale+t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1410 and t1.max_min<1440 then t2.avg_0_sale+t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1440 and t1.max_min<1470 then t2.avg_0a_sale+t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1470 and t1.max_min<1500 then t2.avg_1_sale+t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1500 and t1.max_min<1530 then t2.avg_1a_sale+t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1530 and t1.max_min<1560 then t2.avg_2_sale+t2.avg_2a_sale
     when t1.max_min>=1560 and t1.max_min<1590 then t2.avg_2a_sale
		 when t1.max_min>=1590 then 0 
	end sale_supply_qty
from tmp1 t1
left join rst.rst_sale_supply_avg_copy t2 
	on t1.shop_code=t2.shop_code 
	and t1.product_code=t2.product_code 
	and t1.date_case=t2.date_case 
	and t1.sale_date=t2.sale_date
;


/*
--取所有断货商品
drop table if exists tmp3;
create temp table tmp3 as 
select 
	t3.shop_code,
	t3.product_code,
	t3.check_date,
	t4."case"
from tmp_a t3
left join rst.rst_shop_date_case t4 
	on t3.shop_code=t4.shop_code 
	and t3.check_date=t4."date"
where (t3.inferential_end_qty is null or t3.inferential_end_qty=0) 
	and t3.loss_qty is null
	and t3.check_date=$1 
;

--计算全天销补之和
drop table if exists tmp4;
create temporary table tmp4 as 
select 
	shop_code,
	product_code,
	sale_date,
	date_case,
	(avg_3a_sale+avg_4_sale+avg_4a_sale+avg_5_sale+avg_5a_sale+avg_6_sale+avg_6a_sale+avg_7_sale+avg_7a_sale+avg_8_sale+avg_8a_sale+avg_9_sale+avg_9a_sale+avg_10_sale+avg_10a_sale+avg_11_sale+avg_11a_sale+avg_12_sale+avg_12a_sale+avg_13_sale+avg_13a_sale+avg_14_sale+avg_14a_sale+avg_15_sale+avg_15a_sale+avg_16_sale+avg_16a_sale+avg_17_sale+avg_17a_sale+avg_18_sale+avg_18a_sale+avg_19_sale+avg_19a_sale+avg_20_sale+avg_20a_sale+avg_21_sale+avg_21a_sale+avg_22_sale+avg_22a_sale+avg_23_sale+avg_23a_sale+avg_0_sale+avg_0a_sale+avg_1_sale+avg_1a_sale+avg_2_sale+avg_2a_sale) sale_supply_qty
from rst.rst_sale_supply_avg_copy
where sale_date=$1
;

--计算销补
drop table if exists tmp5;
create temporary table tmp5 as 
select 
	t5.shop_code,
	t5.product_code,
	t5.check_date,
	t6.sale_supply_qty
from tmp3 t5
left join tmp4 t6 
	on t5.shop_code=t6.shop_code 
	and t5.product_code=t6.product_code 
	and t5."case"=t6.date_case 
	--and date_trunc('week',t5.check_date)=t6.sale_date
	and t5.check_date=t6.sale_date
;*/
/*
drop table if exists tmp5; 
create temp table tmp5 as 
select t3.shop_code,
t3.product_code,
t3.check_date,
(t6.avg_3a_sale+t6.avg_4_sale+t6.avg_4a_sale+t6.avg_5_sale+t6.avg_5a_sale+t6.avg_6_sale+t6.avg_6a_sale+t6.avg_7_sale+t6.avg_7a_sale+t6.avg_8_sale+t6.avg_8a_sale+t6.avg_9_sale+t6.avg_9a_sale+t6.avg_10_sale+t6.avg_10a_sale+t6.avg_11_sale+t6.avg_11a_sale+t6.avg_12_sale+t6.avg_12a_sale+t6.avg_13_sale+t6.avg_13a_sale+t6.avg_14_sale+t6.avg_14a_sale+t6.avg_15_sale+t6.avg_15a_sale+t6.avg_16_sale+t6.avg_16a_sale+t6.avg_17_sale+t6.avg_17a_sale+t6.avg_18_sale+t6.avg_18a_sale+t6.avg_19_sale+t6.avg_19a_sale+t6.avg_20_sale+t6.avg_20a_sale+t6.avg_21_sale+t6.avg_21a_sale+t6.avg_22_sale+t6.avg_22a_sale+t6.avg_23_sale+t6.avg_23a_sale+t6.avg_0_sale+t6.avg_0a_sale+t6.avg_1_sale+t6.avg_1a_sale+t6.avg_2_sale+t6.avg_2a_sale) sale_supply_qty
from rst.rst_inventory t3
left join rst.rst_shop_date_case t4 on t3.shop_code=t4.shop_code and t3.check_date=t4."date"
left join rst.rst_sale_supply_avg_copy t6 on t3.shop_code=t6.shop_code and t3.product_code=t6.product_code and t4."case"=t6.date_case and date_trunc('week',t3.check_date)=t6.sale_date
--left join rst.rst_sale_supply_avg_copy t6 on t3.shop_code=t6.shop_code and t3.product_code=t6.product_code and t4."case"=t6.date_case and t6.sale_date='2017-07-06'::date
where (t3.inferential_end_qty is null or t3.inferential_end_qty=0) 
	--and t3.check_date='2017-07-06'::date 
	and t3.check_date=$1
;
*/

--合计当天调拨出库
drop table if exists tmp6a;
create temporary table tmp6a as 
select 
	t1.shop_code_out shop_code,
	t1.product_code,
	t1.allot_date,
	sum(t1.qty) qty
from edw.fct_allot t1 
where t1.allot_date=$1
group by 
	t1.shop_code_out,
	t1.product_code,
	t1.allot_date
;

--合计当天调拨入库
drop table if exists tmp6b;
create temporary table tmp6b as 
select 
	t1.shop_code_in shop_code,
	t1.product_code,
	t1.allot_date,
	sum(t1.qty) qty
from edw.fct_allot t1
where t1.allot_date=$1
group by 
	t1.shop_code_in,
	t1.product_code,
	t1.allot_date
;

--合并数据
drop table if exists tmp6;
create temporary table tmp6 as 
select 
	t7.shop_code,
	t7.product_code,
	t7.check_date,
	t7.begin_qty,
	t7.in_qty,
	t7.group_buying_qty,
	t7.sale_qty,
	t8.sale_supply_qty,
	t7.loss_qty,
	t7.end_qty,
	case when (t12.product_type='拌菜产品' or t13.shop_code is not null)
			and coalesce(t7.begin_qty,0)+coalesce(t7.in_qty,0)+coalesce(t11.qty,0)-coalesce(t10.qty,0)-coalesce(t7.loss_qty,0)-coalesce(t7.inferential_end_qty,0)>=0
		then coalesce(t7.begin_qty,0)+coalesce(t7.in_qty,0)+coalesce(t11.qty,0)-coalesce(t10.qty,0)-coalesce(t7.loss_qty,0)-coalesce(t7.inferential_end_qty,0)
		when (t12.product_type='拌菜产品' or t13.shop_code is not null)
			and coalesce(t7.begin_qty,0)+coalesce(t7.in_qty,0)+coalesce(t11.qty,0)-coalesce(t10.qty,0)-coalesce(t7.loss_qty,0)-coalesce(t7.inferential_end_qty,0)<0
		then 0
		when t12.product_type<>'拌菜产品' and t13.shop_code is null
		then coalesce(t7.sale_qty,0)+coalesce(t8.sale_supply_qty,0)
	end sale_total_qty,
	t7.inferential_end_qty,
	t7.taste_give_qty
from tmp_a t7
left join tmp2 t8 
	on t7.shop_code=t8.shop_code 
	and t7.product_code=t8.product_code 
	and t7.check_date=t8.sale_date
--left join tmp5 t9
--	on t7.shop_code=t9.shop_code 
--	and t7.product_code=t9.product_code
--	and t7.check_date=t9.check_date
left join tmp6a t10 
	on t7.shop_code=t10.shop_code
	and t7.product_code=t10.product_code
	and t7.check_date=t10.allot_date
left join tmp6b t11 
	on t7.shop_code=t11.shop_code
	and t7.product_code=t11.product_code
	and t7.check_date=t11.allot_date
left join rst.rst_product t12 
	on t7.product_code=t12.product_code
left join rst.rst_shop_pos_inaccurate t13
	on t7.shop_code=t13.shop_code
where t7.check_date=$1
;

--GET DIAGNOSTICS v_count = ROW_COUNT;
--raise notice 'tmp6: %', v_count;

--删除品尝赠送、团购数量
drop table if exists tmp7;
create temporary table tmp7 as 
select 
	t11.shop_code,
	t11.product_code,
	t11.check_date,
	t11.begin_qty,
	t11.in_qty,
	t11.group_buying_qty,
	t11.sale_qty,
	t11.sale_supply_qty,
	t11.loss_qty,
	t11.end_qty,
	case when t11.sale_total_qty-coalesce(t11.taste_give_qty,0)-coalesce(t12.group_buying_qty,0)<0 then 0
			 else t11.sale_total_qty-coalesce(t11.taste_give_qty,0)-coalesce(t12.group_buying_qty,0)
	end sale_total_qty,
	t11.inferential_end_qty,
	t11.taste_give_qty
from tmp6 t11
left join rst.rst_inventory t12
	on t11.shop_code=t12.shop_code 
	and t11.product_code=t12.product_code 
	and t11.check_date=(t12.check_date+'2 day'::interval)::date
;


--活动商品使用recent7替代sale_total_qty
drop table if exists tmp8;
create temporary table tmp8 as 
select 
	t1.shop_code, 
	t1.product_code,
	t1.check_date, 
	t1.begin_qty, 
	t1.in_qty, 
	t1.group_buying_qty, 
	t1.sale_qty, 
	t1.sale_supply_qty, 
	t1.loss_qty, 
	t1.end_qty, 
	coalesce(t3.recent_7,t1.sale_total_qty) sale_total_qty, 
	t1.inferential_end_qty, 
	t1.taste_give_qty
from tmp7 t1 
left join rst.rst_activity t2 
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
	and t1.check_date=t2.sale_date
left join rst.rst_sale_feat t3
	on t2.shop_code=t3.shop_code
	and t2.product_code=t3.product_code
	and t2.sale_date=t3.sale_date
where t1.check_date=$1
;


--插入
delete from rst.rst_inventory where check_date=$1;
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
	inferential_end_qty,
	taste_give_qty
)
select 
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
	inferential_end_qty,
	taste_give_qty
from tmp8
;

drop table if exists tmp_a;
drop table if exists tmp1;
drop table if exists tmp2;
drop table if exists tmp3;
drop table if exists tmp4;
drop table if exists tmp5;
drop table if exists tmp6a;
drop table if exists tmp6b;
drop table if exists tmp6;
drop table if exists tmp7;
drop table if exists tmp8;

	RETURN 1;
END

$function$
;

```