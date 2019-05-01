# date_case_update
```
CREATE OR REPLACE FUNCTION rst.p_rst_shop_date_case_update()
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--更新rst_sale_supply_sum
--===========================================================================================
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t1.shop_code,
	t1.product_code,
	coalesce(t2."case",t1.date_case) date_case,
	t1.sale_date,
	t1.max_min,
	t1.sum_3_sale,
	t1.sum_3A_sale,
	t1.sum_4_sale,
	t1.sum_4A_sale,
	t1.sum_5_sale,
	t1.sum_5A_sale,
	t1.sum_6_sale,
	t1.sum_6A_sale,
	t1.sum_7_sale,
	t1.sum_7A_sale,
	t1.sum_8_sale,
	t1.sum_8A_sale,
	t1.sum_9_sale,
	t1.sum_9A_sale,
	t1.sum_10_sale,
	t1.sum_10A_sale,
	t1.sum_11_sale,
	t1.sum_11A_sale,
	t1.sum_12_sale,
	t1.sum_12A_sale,
	t1.sum_13_sale,
	t1.sum_13A_sale,
	t1.sum_14_sale,
	t1.sum_14A_sale,
	t1.sum_15_sale,
	t1.sum_15A_sale,
	t1.sum_16_sale,
	t1.sum_16A_sale,
	t1.sum_17_sale,
	t1.sum_17A_sale,
	t1.sum_18_sale,
	t1.sum_18A_sale,
	t1.sum_19_sale,
	t1.sum_19A_sale,
	t1.sum_20_sale,
	t1.sum_20A_sale,
	t1.sum_21_sale,
	t1.sum_21A_sale,
	t1.sum_22_sale,
	t1.sum_22A_sale,
	t1.sum_23_sale,
	t1.sum_23A_sale,
	t1.sum_0_sale,
	t1.sum_0A_sale,
	t1.sum_1_sale,
	t1.sum_1A_sale,
	t1.sum_2_sale,
	t1.sum_2A_sale,
	t1.end_qty
from rst.rst_sale_supply_sum t1 
left join rst.rst_shop_date_case t2 on t1.shop_code=t2.shop_code and t1.sale_date=t2."date"
;

truncate table rst.rst_sale_supply_sum;
insert into rst.rst_sale_supply_sum 
(
	shop_code,
	product_code,
	date_case,
	sale_date,
	max_min,
	sum_3_sale,
	sum_3A_sale,
	sum_4_sale,
	sum_4A_sale,
	sum_5_sale,
	sum_5A_sale,
	sum_6_sale,
	sum_6A_sale,
	sum_7_sale,
	sum_7A_sale,
	sum_8_sale,
	sum_8A_sale,
	sum_9_sale,
	sum_9A_sale,
	sum_10_sale,
	sum_10A_sale,
	sum_11_sale,
	sum_11A_sale,
	sum_12_sale,
	sum_12A_sale,
	sum_13_sale,
	sum_13A_sale,
	sum_14_sale,
	sum_14A_sale,
	sum_15_sale,
	sum_15A_sale,
	sum_16_sale,
	sum_16A_sale,
	sum_17_sale,
	sum_17A_sale,
	sum_18_sale,
	sum_18A_sale,
	sum_19_sale,
	sum_19A_sale,
	sum_20_sale,
	sum_20A_sale,
	sum_21_sale,
	sum_21A_sale,
	sum_22_sale,
	sum_22A_sale,
	sum_23_sale,
	sum_23A_sale,
	sum_0_sale,
	sum_0A_sale,
	sum_1_sale,
	sum_1A_sale,
	sum_2_sale,
	sum_2A_sale,
	end_qty
) 
select 
	shop_code,
	product_code,
	date_case,
	sale_date,
	max_min,
	sum_3_sale,
	sum_3A_sale,
	sum_4_sale,
	sum_4A_sale,
	sum_5_sale,
	sum_5A_sale,
	sum_6_sale,
	sum_6A_sale,
	sum_7_sale,
	sum_7A_sale,
	sum_8_sale,
	sum_8A_sale,
	sum_9_sale,
	sum_9A_sale,
	sum_10_sale,
	sum_10A_sale,
	sum_11_sale,
	sum_11A_sale,
	sum_12_sale,
	sum_12A_sale,
	sum_13_sale,
	sum_13A_sale,
	sum_14_sale,
	sum_14A_sale,
	sum_15_sale,
	sum_15A_sale,
	sum_16_sale,
	sum_16A_sale,
	sum_17_sale,
	sum_17A_sale,
	sum_18_sale,
	sum_18A_sale,
	sum_19_sale,
	sum_19A_sale,
	sum_20_sale,
	sum_20A_sale,
	sum_21_sale,
	sum_21A_sale,
	sum_22_sale,
	sum_22A_sale,
	sum_23_sale,
	sum_23A_sale,
	sum_0_sale,
	sum_0A_sale,
	sum_1_sale,
	sum_1A_sale,
	sum_2_sale,
	sum_2A_sale,
	end_qty
from tmp1
;

drop table if exists tmp1;


--更新rst_turnover_feat
--=======================================================================================

drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t1.shop_code,
	t1.sale_date,
	coalesce(t2.case,t1.date_case) date_case,
	t1.turnover,
	t1.pre,
	t1.temp,
	t1.recent_3,
	t1.recent_7
from rst.rst_turnover_feat t1
left join rst.rst_shop_date_case t2 on t1.shop_code=t2.shop_code and t1.sale_date=t2.date
where t1.sale_date<=(current_date+'6 day'::interval)::date
;

truncate table rst.rst_turnover_feat;
insert into rst.rst_turnover_feat
(
	shop_code,
	sale_date,
	date_case,
	turnover,
	pre,
	temp,
	recent_3,
	recent_7
)
select 
	shop_code,
	sale_date,
	date_case,
	turnover,
	pre,
	temp,
	recent_3,
	recent_7
from tmp1
;

drop table if exists tmp1;


--更新rst_sale_feat
--=======================================================================================

drop table if exists tmp1;
create temporary table tmp1 as
select 
	t1.shop_code,
	t1.product_code,
	t1.sale_date,
	coalesce(t2.case,t1.date_case) date_case,
	t1.turnover,
	t1.temp,
	t1.qty,
	t1.recent_3,
	t1.recent_7
from rst.rst_sale_feat t1
left join rst.rst_shop_date_case t2 on t1.shop_code=t2.shop_code and t1.sale_date=t2.date
where t1.sale_date<=(current_date+'6 day'::interval)::date
;

truncate table rst.rst_sale_feat;
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
from tmp1
;

drop table if exists tmp1;


	RETURN 1;
END

$function$
;

```