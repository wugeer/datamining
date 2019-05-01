# sale_supply_sum
```
CREATE OR REPLACE FUNCTION rst.p_rst_sale_supply_sum()
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--建立在售商品的时间序列
drop table if exists tmp0;
create temporary table tmp0 as 
select 
	distinct shop_code,
	product_code,
	sale_date::date
from rst.rst_product_onsale
cross join (select generate_series('2016-07-01'::date,current_date-1,'1 day') sale_date) a
;

--取当天最大销售时间
drop table if exists tmp1;
create temp table tmp1 as 
select 
	shop_code,
	product_code,
	sale_date,
	max(sale_min) max_min
from rst.rst_pos
where sale_date>='2016-07-01'::date
group by 
	shop_code,
	product_code,
	sale_date
;

--汇总
drop table if exists tmp2;
create temp table tmp2 as 
(
	select 
		t0.shop_code,
		t0.product_code,
		t0.sale_date,
		t4.sale_min,
		t4.qty,
		t1.max_min,
		t2."case" date_case,
		t3.end_qty
	from tmp0 t0
	left join tmp1 t1 on t0.shop_code=t1.shop_code and t0.product_code=t1.product_code and t0.sale_date=t1.sale_date 
	left join rst.rst_shop_date_case t2 on t0.shop_code=t2.shop_code and t0.sale_date=t2."date" 
	left join rst.rst_inventory t3 on t0.shop_code=t3.shop_code and t0.product_code=t3.product_code and t0.sale_date=t3.check_date
	left join rst.rst_pos t4 on t0.shop_code=t4.shop_code and t0.product_code=t4.product_code and t0.sale_date=t4.sale_date 
	where t0.sale_date<='2018-07-17'::date
)
union
(
	select 
		t5.shop_code,
		t5.product_code,
		t5.sale_date,
		t9.sale_min,
		t9.qty,
		t6.max_min,
		t7."case" date_case,
		t8.inferential_end_qty end_qty
	from tmp0 t5
	left join tmp1 t6 on t5.shop_code=t6.shop_code and t5.product_code=t6.product_code and t5.sale_date=t6.sale_date 
	left join rst.rst_shop_date_case t7 on t5.shop_code=t7.shop_code and t5.sale_date=t7."date" 
	left join rst.rst_inventory t8 on t5.shop_code=t8.shop_code and t5.product_code=t8.product_code and t5.sale_date=t8.check_date
	left join rst.rst_pos t9 on t5.shop_code=t9.shop_code and t5.product_code=t9.product_code and t5.sale_date=t9.sale_date 
	where t5.sale_date>='2018-07-18'::date
)
;


--truncate table rst.rst_sale_supply_sum;
--insert into rst.rst_sale_supply_sum 
drop table if exists rst.rst_sale_supply_sum;
create table rst.rst_sale_supply_sum
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
) as
select 
	shop_code,
	product_code,
	date_case,
	sale_date,
	coalesce(max_min,180) max_min,
	coalesce(sum(case when sale_min>=180 then qty end),0) sum_3_sale,
	sum(case when sale_min>=210 then qty end) sum_3A_sale,
	sum(case when sale_min>=240 then qty end) sum_4_sale,
	sum(case when sale_min>=270 then qty end) sum_4A_sale,
	sum(case when sale_min>=300 then qty end) sum_5_sale,
	sum(case when sale_min>=330 then qty end) sum_5A_sale,
	sum(case when sale_min>=360 then qty end) sum_6_sale,
	sum(case when sale_min>=390 then qty end) sum_6A_sale,
	sum(case when sale_min>=420 then qty end) sum_7_sale,
	sum(case when sale_min>=450 then qty end) sum_7A_sale,
	sum(case when sale_min>=480 then qty end) sum_8_sale,
	sum(case when sale_min>=510 then qty end) sum_8A_sale,
	sum(case when sale_min>=540 then qty end) sum_9_sale,
	sum(case when sale_min>=570 then qty end) sum_9A_sale,
	sum(case when sale_min>=600 then qty end) sum_10_sale,
	sum(case when sale_min>=630 then qty end) sum_10A_sale,
	sum(case when sale_min>=660 then qty end) sum_11_sale,
	sum(case when sale_min>=690 then qty end) sum_11A_sale,
	sum(case when sale_min>=720 then qty end) sum_12_sale,
	sum(case when sale_min>=750 then qty end) sum_12A_sale,
	sum(case when sale_min>=780 then qty end) sum_13_sale,
	sum(case when sale_min>=810 then qty end) sum_13A_sale,
	sum(case when sale_min>=840 then qty end) sum_14_sale,
	sum(case when sale_min>=870 then qty end) sum_14A_sale,
	sum(case when sale_min>=900 then qty end) sum_15_sale,
	sum(case when sale_min>=930 then qty end) sum_15A_sale,
	sum(case when sale_min>=960 then qty end) sum_16_sale,
	sum(case when sale_min>=990 then qty end) sum_16A_sale,
	sum(case when sale_min>=1020 then qty end) sum_17_sale,
	sum(case when sale_min>=1050 then qty end) sum_17A_sale,
	sum(case when sale_min>=1080 then qty end) sum_18_sale,
	sum(case when sale_min>=1110 then qty end) sum_18A_sale,
	sum(case when sale_min>=1140 then qty end) sum_19_sale,
	sum(case when sale_min>=1170 then qty end) sum_19A_sale,
	sum(case when sale_min>=1200 then qty end) sum_20_sale,
	sum(case when sale_min>=1230 then qty end) sum_20A_sale,
	sum(case when sale_min>=1260 then qty end) sum_21_sale,
	sum(case when sale_min>=1290 then qty end) sum_21A_sale,
	sum(case when sale_min>=1320 then qty end) sum_22_sale,
	sum(case when sale_min>=1350 then qty end) sum_22A_sale,
	sum(case when sale_min>=1380 then qty end) sum_23_sale,
	sum(case when sale_min>=1410 then qty end) sum_23A_sale,
	sum(case when sale_min>=1440 then qty end) sum_0_sale,
	sum(case when sale_min>=1470 then qty end) sum_0A_sale,
	sum(case when sale_min>=1500 then qty end) sum_1_sale,
	sum(case when sale_min>=1530 then qty end) sum_1A_sale,
	sum(case when sale_min>=1560 then qty end) sum_2_sale,
	sum(case when sale_min>=1590 then qty end) sum_2A_sale,
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

--删除连续7天以上没有销售记录的商品
drop table if exists tmp8;
create temporary table tmp8 as 
select 
	shop_code,
	product_code,
	sale_date,
	lead(sale_date,1,'2016-06-30'::date) over(partition by shop_code,product_code order by sale_date desc) before_date
from rst.rst_sale_supply_sum
where max_min<>180
;
--select * from tmp8

drop table if exists tmp9;
create temporary table tmp9 as 
select 
	shop_code,
	product_code,
	sale_date,
	before_date
from tmp8
where sale_date-before_date>='8 day'::interval
;
--select * from tmp9

delete from rst.rst_sale_supply_sum t10
using tmp9 t11
where (t10.sale_date<t11.sale_date and t10.sale_date>t11.before_date)
	and t10.shop_code=t11.shop_code and t10.product_code=t11.product_code
	and t10.max_min=180
;

drop table if exists tmp0;
drop table if exists tmp1;
drop table if exists tmp2;
drop table if exists tmp8;
drop table if exists tmp9;

	RETURN 1;
END

$function$
;

```