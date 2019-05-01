# rst_propuct_bill_shop
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_bill_shop(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--取在售门店与试点门店交集
drop table if exists tmp0;
create temporary table tmp0 as 
select 
	distinct t1.shop_code
from rst.rst_product_onsale t1 
inner join rst.rst_shop_test t2 
	on t1.shop_code=t2.shop_code
;


--计算在售试点门店辅料类以及商品编码不以'05'开头的商品，近四周同期平均要货数
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t1.shop_code,
	t1.product_code,
	$1 bill_date,
	($1+'2 day'::interval)::date arrive_date,
	sum(coalesce(t1.qty,0))/4 qty
from rst.rst_order t1
left join rst.rst_product t2 
	on t1.product_code=t2.product_code
inner join tmp0 t3 
	on t1.shop_code=t3.shop_code
where t1.order_date>=($1-'1 month'::interval)::date 
	and t1.order_date<$1
	and extract(dow from t1.order_date)=extract(dow from $1)
	and t2.product_type is not null 
	and t2.product_type<>'物料类'
	and t2.product_code not like '05%'
group by 
	t1.shop_code,
	t1.product_code,
	$1,
	($1+'2 day'::interval)::date
;


--加入近两天有过要货的'02'商品
drop table if exists tmp1_a;
create temporary table tmp1_a as 
select 
	t1.shop_code,
	t1.product_code,
	t1.bill_date,
	t1.arrive_date,
	t1.qty
from tmp1 t1
union
select 
	distinct t2.shop_code,
	t2.product_code,
	$1 bill_date,
	($1+'2 day'::interval)::date arrive_date,
	0 qty
from rst.rst_product_bill_shop t2
inner join tmp0 t3
	on t2.shop_code=t3.shop_code
left join rst.rst_product t4
	on t2.product_code=t4.product_code
where t2.bill_date>=($1-'2 day'::interval)::date
	and t2.bill_date<$1
	and t4.product_type is not null 
	and t4.product_type<>'物料类'
	and t4.product_code not like '05%'
	and not exists (select 1 from tmp1 t5
					where t2.shop_code=t5.shop_code
						and t2.product_code=t5.product_code)
;


--调整为最小起送量的倍数（不到最小起送量按最小起送量计算），剔除昨日下架商品
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	t3.shop_code,
	t4.shop_name,
	t3.bill_date,
	t3.arrive_date,
	t5.product_type,
	t3.product_code,
	t5.product_name,
	t5.measure_unit,
	t5.min_qty,
	null::numeric min_display_qty,
	null::numeric price_sale,
	t6.price_wholesale,
	t7.qty d0_end_qty,
	t8.final_qty d1_in_qty,
	t9.final_qty d2_in_qty,
	null::numeric d2_end_qty_predict,
	null::numeric optimal_purchase,
	case when t3.qty%t5.min_qty=0 then t3.qty
		 else (t3.qty-t3.qty%t5.min_qty+t5.min_qty) 
	end final_purchase,
	null::numeric d3_sale_predict,
	now() create_time,
	t4.district_manager_code,
	t4.district_manager_name,
	t4.district_director_code,
	t4.district_director_name
from tmp1_a t3
left join rst.rst_shop t4 
	on t3.shop_code=t4.shop_code 
left join rst.rst_product t5 
	on t3.product_code=t5.product_code
left join rst.rst_price t6 
	on t3.shop_code=t6.shop_code 
	and t3.product_code=t6.product_code
left join rst.rst_stock t7
	on t3.shop_code=t7.shop_code
	and t3.product_code=t7.product_code
	and t3.bill_date=(t7.check_date+'1 day'::interval)::date
left join rst.rst_product_arrive t8
	on t3.shop_code=t8.shop_code
	and t3.product_code=t8.product_code
	and t3.arrive_date=(t8.arrive_date+'2 day'::interval)::date
left join rst.rst_product_arrive t9
	on t3.shop_code=t9.shop_code
	and t3.product_code=t9.product_code
	and t3.arrive_date=(t9.arrive_date+'1 day'::interval)::date
where t5.prohibit_order<>'1' 
	and not exists(
		select 1 from rst.rst_off_product t10 
		where t3.shop_code=t10.shop_code
			and t3.product_code=t10.product_code
			and t10.create_time::date=($1-'1 day'::interval)::date
		)
--where t3.qty<>0
;


--合并模型与辅料要货
delete from rst.rst_product_bill_shop where bill_date=$1;
insert into rst.rst_product_bill_shop 
(
	shop_code,
	shop_name,
	bill_date,
	arrive_date,
	product_type,
	product_code,
	product_name,
	measure_unit,
	min_qty,
	min_display_qty,
	price_sale,
	price_wholesale,
	d0_end_qty,
	d1_in_qty,
	d2_in_qty,
	d2_end_qty_predict,
	optimal_purchase,
	final_purchase,
	d3_sale_predict,
	create_time,
	district_manager_code,
	district_manager_name,
	district_director_code,
	district_director_name
) 
(
select 
	t8.shop_code,
	t8.shop_name,
	t8.bill_date,
	t8.arrive_date,
	t8.product_type,
	t8.product_code,
	t8.product_name,
	t8.measure_unit,
	t8.min_qty,
	t8.min_display_qty,
	t10.price_sale,
	t10.price_wholesale,
	t11.qty,
	t8.d1_in_qty,
	t8.d2_in_qty,
	t8.d2_end_qty_predict,
	t8.optimal_purchase,
	t8.final_purchase,
	t8.d3_sale_predict,
	t8.create_time,
	t9.district_manager_code,
	t9.district_manager_name,
	t9.district_director_code,
	t9.district_director_name
from rst.rst_product_bill_model t8
left join rst.rst_shop t9 
	on t8.shop_code=t9.shop_code 
left join rst.rst_price t10 
	on t8.shop_code=t10.shop_code
	and t8.product_code=t10.product_code
left join rst.rst_stock t11
	on t8.shop_code=t11.shop_code
	and t8.product_code=t11.product_code
	and t8.bill_date=(t11.check_date+'1 day'::interval)::date
left join rst.rst_product t12
	on t8.product_code=t12.product_code
where t8.bill_date=$1
	and t12.prohibit_order<>'1'
)
union 
(
select 
	t13.shop_code,
	t13.shop_name,
	t13.bill_date,
	t13.arrive_date,
	t13.product_type,
	t13.product_code,
	t13.product_name,
	t13.measure_unit,
	t13.min_qty,
	t13.min_display_qty,
	t13.price_sale,
	t13.price_wholesale,
	t13.d0_end_qty,
	t13.d1_in_qty,
	t13.d2_in_qty,
	t13.d2_end_qty_predict,
	t13.optimal_purchase,
	t13.final_purchase,
	t13.d3_sale_predict,
	t13.create_time,
	t13.district_manager_code,
	t13.district_manager_name,
	t13.district_director_code,
	t13.district_director_name
from tmp2 t13
where t13.bill_date=$1
)
;


--删除暂停营业第一天的要货单
delete from rst.rst_product_bill_shop t1
where exists(select 1 from rst.rst_shop_pause_history t2 
		where t1.shop_code=t2.shop_code 
			and t1.bill_date=t2.pause_time_point
			and t2.pause_time_point=$1)
;

drop table if exists tmp0;
drop table if exists tmp1;
drop table if exists tmp1_a;
drop table if exists tmp2;

	RETURN 1;
END

$function$
;

```