# rst_digital_warehouse
```
CREATE OR REPLACE FUNCTION rst.p_rst_digital_warehouse(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN
/*
drop table if exists tmp_a;
create temporary table tmp_a as 
select t1.shop_code,
	   t1.product_code,
	   t1.check_date,
	   t2.shelf_life,
	   coalesce(t1.inferential_end_qty,0) end_qty,
	   case when t2.shelf_life='1 day'::interval then coalesce(t1.inferential_end_qty,0)::varchar
	   		when t2.shelf_life='2 day'::interval then 0||','||coalesce(t1.inferential_end_qty,0)
	   		when t2.shelf_life='3 day'::interval then 0||','||0||','||coalesce(t1.inferential_end_qty,0)
	   		when t2.shelf_life='4 day'::interval then 0||','||0||','||0||','||coalesce(t1.inferential_end_qty,0)
	   		when t2.shelf_life='5 day'::interval then 0||','||0||','||0||','||0||','||coalesce(t1.inferential_end_qty,0)
	   		when t2.shelf_life='6 day'::interval then 0||','||0||','||0||','||0||','||0||','||coalesce(t1.inferential_end_qty,0)
	   		when t2.shelf_life='7 day'::interval then 0||','||0||','||0||','||0||','||0||','||0||','||coalesce(t1.inferential_end_qty,0)
	   end digital_warehouse
from rst.rst_inventory t1
left join rst.rst_product t2 on t1.product_code=t2.product_code
where t2.shelf_life<='7 day'::interval and t1.check_date='2018-07-11'::date
;
--select * from tmp_a 
*/

--取出inventory中货架期小于7天的商品
drop table if exists tmp1;
create temporary table tmp1 as 
select t1.shop_code,
	   t1.product_code,
	   t1.check_date,
	   t2.shelf_life,
	   t1.inferential_end_qty,
	   t1.in_qty,
	   t1.sale_qty,
	   t1.loss_qty
from rst.rst_inventory t1
left join rst.rst_shelf_life t2 
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
where t2.shelf_life<='7 day'::interval 
	and t1.check_date=$1
;


--取出昨日库龄
drop table if exists tmp2a;
create temporary table tmp2a as 
select shop_code,
	   product_code,
	   check_date,
	   shelf_life,
	   end_qty,
	   digital_warehouse
from rst.rst_digital_warehouse
where shelf_life<='7 day'::interval 
	and check_date=($1-'1 day'::interval)::date
	and end_qty<>0
;

--合并digital_warehouse和inventory,计算期初库存
drop table if exists tmp2;
create temporary table tmp2 as 
select coalesce(t3.shop_code,t4.shop_code) shop_code,
	   coalesce(t3.product_code,t4.product_code) product_code,
	   coalesce((t3.check_date+'1 day'::interval)::date,t4.check_date) check_date,
	   coalesce(t3.shelf_life,t4.shelf_life) shelf_life,
	   case when t3.shelf_life>'1 day'::interval then split_part(t3.digital_warehouse,',',2)::numeric
	 		when t3.shelf_life='1 day'::interval then 0
	 		else null end day_1_qty,
	   case when t3.shelf_life>'2 day'::interval then split_part(t3.digital_warehouse,',',3)::numeric
	 		when t3.shelf_life='2 day'::interval then 0
	 		else null end day_2_qty,
	   case when t3.shelf_life>'3 day'::interval then split_part(t3.digital_warehouse,',',4)::numeric
	 		when t3.shelf_life='3 day'::interval then 0
	 		else null end day_3_qty,
	   case when t3.shelf_life>'4 day'::interval then split_part(t3.digital_warehouse,',',5)::numeric
	 		when t3.shelf_life='4 day'::interval then 0
	 		else null end day_4_qty,
	   case when t3.shelf_life>'5 day'::interval then split_part(t3.digital_warehouse,',',6)::numeric
	 		when t3.shelf_life='5 day'::interval then 0
	 		else null end day_5_qty,
	   case when t3.shelf_life>'6 day'::interval then split_part(t3.digital_warehouse,',',7)::numeric
	 		when t3.shelf_life='6 day'::interval then 0
	 		else null end day_6_qty,
	   case when t3.shelf_life='7 day'::interval then 0
	 		else null end day_7_qty,
	   coalesce(t4.sale_qty,0) sale_qty,
	   coalesce(t4.in_qty,0) in_qty,
	   coalesce(t4.inferential_end_qty,0) inferential_end_qty
from tmp2a t3
full join tmp1 t4 
	on t3.shop_code=t4.shop_code 
	and t3.product_code=t4.product_code 
;
--select * from tmp2 

--出入库、日末库存为空即0
drop table if exists tmp3;
create temporary table tmp3 as 
select shop_code,
	   product_code,
	   check_date,
	   shelf_life,
	   day_1_qty,
	   day_2_qty,
	   day_3_qty,
	   day_4_qty,
	   day_5_qty,
	   day_6_qty,
	   day_7_qty,
	   case when coalesce(day_1_qty,0)+coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)+coalesce(in_qty,0)-coalesce(sale_qty,0)>coalesce(inferential_end_qty,0) 
	   		then coalesce(day_1_qty,0)+coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)+coalesce(in_qty,0)-coalesce(inferential_end_qty,0)
	   		else coalesce(sale_qty,0) end sale_qty,
	   case when coalesce(day_1_qty,0)+coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)+coalesce(in_qty,0)-coalesce(sale_qty,0)<coalesce(inferential_end_qty,0) 
	   		then coalesce(inferential_end_qty,0)-(coalesce(day_1_qty,0)+coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)+coalesce(in_qty,0)-coalesce(sale_qty,0))
	   		else 0 end extra_qty,
	   coalesce(in_qty,0) in_qty,
	   coalesce(inferential_end_qty,0) inferential_end_qty
from tmp2
;
--select * from tmp3 

--计算入库
drop table if exists tmp4;
create temporary table tmp4 as 
select shop_code,
	   product_code,
	   check_date,
	   shelf_life,
	   case when shelf_life>'1 day'::interval then coalesce(day_1_qty,0)
	   		else in_qty+extra_qty end day_1_qty,
	   case when shelf_life>'2 day'::interval then coalesce(day_2_qty,0)+extra_qty
	   		when shelf_life='2 day'::interval then in_qty+extra_qty
	   		else null end day_2_qty,
	   case when shelf_life>'3 day'::interval then coalesce(day_3_qty,0)
	   		when shelf_life='3 day'::interval then in_qty
	   		else null end day_3_qty,
	   case when shelf_life>'4 day'::interval then coalesce(day_4_qty,0)
	   		when shelf_life='4 day'::interval then in_qty
	   		else null end day_4_qty,
	   case when shelf_life>'5 day'::interval then coalesce(day_5_qty,0)
	   		when shelf_life='5 day'::interval then in_qty
	   		else null end day_5_qty,
	   case when shelf_life>'6 day'::interval then coalesce(day_6_qty,0)
	   		when shelf_life='6 day'::interval then in_qty
	   		else null end day_6_qty,		
	   case when shelf_life='7 day'::interval then in_qty
	   		else null end day_7_qty,
	   sale_qty,
	   inferential_end_qty
from tmp3
;
--select * from tmp4 

--计算出库(累和)
drop table if exists tmp5;
create temporary table tmp5 as 
select shop_code,
	   product_code,
	   check_date,
	   shelf_life,
	   inferential_end_qty,
	   case when day_1_qty-sale_qty<=0 then 0 
	 		when day_1_qty-sale_qty>0 then day_1_qty-sale_qty 
	   end day_1_sum,
	   case when day_1_qty+day_2_qty-sale_qty<=0 then 0
	   		when day_1_qty+day_2_qty-sale_qty>0 then day_1_qty+day_2_qty-sale_qty 
	   end day_2_sum,
	   case when day_1_qty+day_2_qty+day_3_qty-sale_qty<=0 then 0
	   		when day_1_qty+day_2_qty+day_3_qty-sale_qty>0 then day_1_qty+day_2_qty+day_3_qty-sale_qty 
	   end day_3_sum,
	   case when day_1_qty+day_2_qty+day_3_qty+day_4_qty-sale_qty<=0 then 0
	   		when day_1_qty+day_2_qty+day_3_qty+day_4_qty-sale_qty>0 then day_1_qty+day_2_qty+day_3_qty+day_4_qty-sale_qty 
	   end day_4_sum,
	   case when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty-sale_qty<=0 then 0
	   		when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty-sale_qty>0 then day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty-sale_qty 
	   end day_5_sum,
	   case when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty-sale_qty<=0 then 0
	   		when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty-sale_qty>0 then day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty-sale_qty 
	   end day_6_sum,
	   case when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty+day_7_qty-sale_qty<=0 then 0
	   		when day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty+day_7_qty-sale_qty>0 then day_1_qty+day_2_qty+day_3_qty+day_4_qty+day_5_qty+day_6_qty+day_7_qty-sale_qty 
	   end day_7_sum
from tmp4
;
--select * from tmp5 

--展开每日库存
drop table if exists tmp6;
create temporary table tmp6 as 
select shop_code,
	   product_code,
	   check_date,
	   shelf_life,
	   inferential_end_qty,
	   0+day_1_sum day_1_qty,
	   0+day_2_sum-day_1_sum day_2_qty,
	   0+day_3_sum-day_2_sum day_3_qty,
	   0+day_4_sum-day_3_sum day_4_qty,
	   0+day_5_sum-day_4_sum day_5_qty,
	   0+day_6_sum-day_5_sum day_6_qty,
	   0+day_7_sum-day_6_sum day_7_qty
from tmp5
;
--select * from tmp6


--判断货架期是否变动，如变动则调整库龄，增加则补0，减少则压缩减少的天数
drop table if exists tmp7;
create temporary table tmp7 as 
select 
	t7.shop_code,
	t7.product_code,
	t7.check_date,
	t8.shelf_life,
	t7.inferential_end_qty,
	case when t7.shelf_life>'1 day'::interval and t8.shelf_life='1 day'::interval 
			then coalesce(day_1_qty,0)+coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
		else day_1_qty
	end day_1_qty,
	case when t7.shelf_life<'2 day'::interval and t8.shelf_life>='2 day'::interval 
			then 0
		when t7.shelf_life>='2 day'::interval and t8.shelf_life<'2 day'::interval 
			then null 
		when t7.shelf_life>'2 day'::interval and t8.shelf_life='2 day'::interval 
			then coalesce(day_2_qty,0)+coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
		else day_2_qty
	end day_2_qty,
	case when t7.shelf_life<'3 day'::interval and t8.shelf_life>='3 day'::interval 
			then 0
		when t7.shelf_life>='3 day'::interval and t8.shelf_life<'3 day'::interval 
			then null 
		when t7.shelf_life>'3 day'::interval and t8.shelf_life='3 day'::interval 
			then coalesce(day_3_qty,0)+coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
		else day_3_qty
	end day_3_qty,
	case when t7.shelf_life<'4 day'::interval and t8.shelf_life>='4 day'::interval 
			then 0
		when t7.shelf_life>='4 day'::interval and t8.shelf_life<'4 day'::interval 
			then null 
		when t7.shelf_life>'4 day'::interval and t8.shelf_life='4 day'::interval 
			then coalesce(day_4_qty,0)+coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
		else day_4_qty
	end day_4_qty,
	case when t7.shelf_life<'5 day'::interval and t8.shelf_life>='5 day'::interval 
			then 0
		when t7.shelf_life>='5 day'::interval and t8.shelf_life<'5 day'::interval 
			then null 
		when t7.shelf_life>'5 day'::interval and t8.shelf_life='5 day'::interval 
			then coalesce(day_5_qty,0)+coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
		else day_5_qty
	end day_5_qty,
	case when t7.shelf_life<'6 day'::interval and t8.shelf_life>='6 day'::interval 
			then 0
		when t7.shelf_life>='6 day'::interval and t8.shelf_life<'6 day'::interval 
			then null 
		when t7.shelf_life>'6 day'::interval and t8.shelf_life='6 day'::interval 
			then coalesce(day_6_qty,0)+coalesce(day_7_qty,0)
		else day_6_qty
	end day_6_qty,
	case when t7.shelf_life<'7 day'::interval and t8.shelf_life>='7 day'::interval 
			then 0
		when t7.shelf_life>='7 day'::interval and t8.shelf_life<'7 day'::interval 
			then null 
		else day_7_qty
	end day_7_qty
from tmp6 t7
left join rst.rst_shelf_life t8 
	on t7.shop_code=t8.shop_code
	and t7.product_code=t8.product_code
;

--拼接库龄
delete from rst.rst_digital_warehouse where check_date=$1;
insert into rst.rst_digital_warehouse
(
	shop_code,
	product_code,
	check_date,
	shelf_life,
	end_qty,
	digital_warehouse
)
select shop_code,
	   product_code,
	   check_date,
	   shelf_life,
	   inferential_end_qty,
	   case when shelf_life='1 day'::interval then day_1_qty::varchar
	   		when shelf_life='2 day'::interval then day_1_qty||','||day_2_qty
	   		when shelf_life='3 day'::interval then day_1_qty||','||day_2_qty||','||day_3_qty
	   		when shelf_life='4 day'::interval then day_1_qty||','||day_2_qty||','||day_3_qty||','||day_4_qty
	   		when shelf_life='5 day'::interval then day_1_qty||','||day_2_qty||','||day_3_qty||','||day_4_qty||','||day_5_qty
	   		when shelf_life='6 day'::interval then day_1_qty||','||day_2_qty||','||day_3_qty||','||day_4_qty||','||day_5_qty||','||day_6_qty
	   		when shelf_life='7 day'::interval then day_1_qty||','||day_2_qty||','||day_3_qty||','||day_4_qty||','||day_5_qty||','||day_6_qty||','||day_7_qty
	   end digital_warehouse
from tmp7
where check_date=$1
;
--select * from tmp7

--插入常保商品库龄
insert into rst.rst_digital_warehouse
(
	shop_code,
	product_code,
	check_date,
	shelf_life,
	end_qty
)
select 
	t5.shop_code,
	t5.product_code,
	t5.check_date,
	t6.shelf_life,
	t5.inferential_end_qty
from rst.rst_inventory t5 
left join rst.rst_shelf_life t6 
	on t5.shop_code=t6.shop_code
	and t5.product_code=t6.product_code
where t5.check_date=$1
	and t6.shelf_life>'7 day'::interval
;



drop table if exists tmp1;
drop table if exists tmp2;
drop table if exists tmp3;
drop table if exists tmp4;
drop table if exists tmp5;
drop table if exists tmp6;
drop table if exists tmp7a;
drop table if exists tmp7;

	RETURN 1;
END

$function$
;

```