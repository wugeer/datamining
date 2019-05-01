# rst_on_sale
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_onsale(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--取试点门店与昨日模型要货单门店的交集,即智能要货门店
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	distinct t1.shop_code
from rst.rst_product_bill_model t1 
inner join rst.rst_shop_test t2 
	on t1.shop_code=t2.shop_code
where t1.bill_date=$1
;


--非智能要货门店，取近一个月有销售记录的商品或近一个月有库存记录的商品或昨日要货单商品
drop table if exists tmp2;
create temporary table tmp2 as 
(
	select 
		distinct t1.shop_code,
		t1.product_code
	from rst.rst_pos t1
	where t1.sale_date>=($1-'1 month'::interval)::date 
		and t1.sale_date<=$1
		and not exists (select 1 from tmp1 t2 
						where t1.shop_code=t2.shop_code)
)
union
(
	select 
		distinct t3.shop_code,
		t3.product_code
	from rst.rst_stock t3
	where t3.check_date>=($1-'1 month'::interval)::date 
		and t3.check_date<=$1
		and not exists (select 1 from tmp1 t4 
						where t3.shop_code=t4.shop_code)
)
union
(
	select 
		distinct t5.shop_code,
		t5.product_code
	from rst.rst_product_bill_shop t5
	where t5.bill_date=$1
		and t5.del_flag='0'
		and t5.off_flag='0'
		and not exists (select 1 from tmp1 t6 
						where t5.shop_code=t6.shop_code)
)
union
(
	select 
		distinct t7.shop_code,
		t7.product_code
	from rst.rst_product_bill_other t7
	where t7.bill_date=$1
		and t7.goods_type='团购要货'
		and not exists (select 1 from tmp1 t8 
						where t7.shop_code=t8.shop_code)
)
;


--智能要货门店,取前一天要货的商品和前一天上新的商品
drop table if exists tmp3;
create temporary table tmp3 as 
(
	select 
		distinct t1.shop_code,
		t1.product_code
	from rst.rst_product_bill_shop t1
	inner join tmp1 t2 
		on t1.shop_code=t2.shop_code
	where t1.bill_date=$1
		and t1.del_flag='0'
		and t1.off_flag='0'
)
union 
(
	select 
		distinct t3.shop_code,
		t3.product_code
	from rst.rst_product_bill_other t3
	inner join tmp1 t4
		on t3.shop_code=t4.shop_code
	where t3.bill_date=$1
		and t3.goods_type='团购要货'
)
union
(
	select 
		distinct t5.shop_code,
		t5.product_code
	from rst.rst_product_new t5
	inner join tmp1 t6
		on t5.shop_code=t6.shop_code
	where t5.putaway_date=$1
)
;


--合并试点门店与其他门店
drop table if exists tmp4;
create temporary table tmp4 as 
(
	select 
		distinct shop_code,
		product_code
	from tmp2
)
union 
(
	select 
		distinct shop_code,
		product_code
	from tmp3
)
;


--剔除暂停营业的门店
drop table if exists tmp5;
create temporary table tmp5 as 
select 
	t1.shop_code,
	t1.product_code
from tmp4 t1
where not exists(
		select 1 from rst.rst_shop_pause_history t2 
		where t1.shop_code=t2.shop_code 
			and $1>=t2.pause_time_point 
			and $1<=t2.resume_time_point-2)
;


--暂停营业门店，营业前三天的onsale，取暂停前的最大日期
drop table if exists tmp6;
create temporary table tmp6 as 
select 
	t1.shop_code,
	max(t1.sale_date) sale_date
from rst.rst_product_onsale_history t1 
where exists(
		select 1 from rst.rst_shop_pause_history t2 
		where t1.shop_code=t2.shop_code 
			and t1.sale_date<t2.pause_time_point
			and $1=t2.resume_time_point-2)
group by 
	t1.shop_code
;


--合并恢复营业门店和正常营业门店的onsale
drop table if exists tmp7;
create temporary table tmp7 as 
(
	select
		distinct shop_code,
		product_code
	from tmp5
)
union 
(
	select 
		distinct t1.shop_code,
		t1.product_code
	from rst.rst_product_onsale_history t1 
	inner join tmp6 t2 
		on t1.shop_code=t2.shop_code
		and t1.sale_date=t2.sale_date
)
;


--计算每个商品平均进货价
drop table if exists tmp8;
create temporary table tmp8 as 
select 
	product_code,
	avg(price_wholesale) price_wholesale
from ods.ods_price_wholesale
group by 
	product_code
;


--近一个月商品销售额排名
drop table if exists tmp9;
create temporary table tmp9 as 
select 
	t1.shop_code,
	t1.product_code,
	row_number() over(partition by t1.shop_code order by sum(t1.qty*coalesce(t2.price_sale,0)) desc) product_rank
from rst.rst_pos t1 
left join rst.rst_price t2 
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
left join rst.rst_product t3
	on t1.product_code=t3.product_code
where t1.sale_date>=($1-'1 month'::interval)::date
	and t1.sale_date<=$1
	and t3.product_type<>'长效品'
group by 
	t1.shop_code,
	t1.product_code
;

--对商品进行剔除，插入数据
truncate table rst.rst_product_onsale;
insert into rst.rst_product_onsale 
(
	shop_code,
	shop_name,
	product_code,
	product_name,
	product_type,
	price_sale,
	price_wholesale,
	shelf_life,
	measure_unit,
	min_qty,
	min_display_qty,
	best_life,
	is_main
) 
select 
	t3.shop_code,
	t4.shop_name,
	t3.product_code,
	t5.product_name,
	t5.product_type,
	t6.price_sale,
	case 
		when t10.price_wholesale is not null then t10.price_wholesale
		when t10.price_wholesale is null and t11.price_wholesale is not null then t11.price_wholesale
		when t10.price_wholesale is null and t11.price_wholesale is null and t6.price_sale=t6.price_wholesale then t6.price_sale*0.65
		else t6.price_wholesale 
	end price_wholesale,
	t8.shelf_life,
	t5.measure_unit,
	t5.min_qty,
	coalesce(t7.min_display_qty,0) min_display_qty,
	t8.best_life,
	case 
		when t5.product_name like '%鸭脖%' or t5.product_name like '%鸭翅%' or t5.product_name like '%鸭锁骨%' then 't'
		when t12.product_rank<=20 then 't'
		else 'f'
	end is_main
from tmp7 t3
left join edw.dim_shop t4 
	on t3.shop_code=t4.shop_code
left join edw.dim_product t5 
	on t3.product_code=t5.product_code
left join edw.dim_price t6 
	on t3.shop_code=t6.shop_code 
	and t3.product_code=t6.product_code
left join rst.rst_product_display t7 
	on t3.shop_code=t7.shop_code 
	and t3.product_code=t7.product_code 
left join rst.rst_shelf_life t8 
	on t3.shop_code=t8.shop_code 
	and t3.product_code=t8.product_code 
left join ods.ods_price_wholesale t10
	on t3.shop_code=t10.shop_code
	and t3.product_code=t10.product_code
left join tmp8 t11
	on t3.product_code=t11.product_code
left join tmp9 t12
	on t3.shop_code=t12.shop_code
	and t3.product_code=t12.product_code
where t3.product_code like '05%' 
	and t5.product_type <>'辅料类' 
	and t5.product_type <>'物料类' 
	and t5.product_type is not null
	and t4.is_valid='1'
	and not exists(
		select 1 from rst.rst_off_product t9 
			where t3.shop_code=t9.shop_code
				and t3.product_code=t9.product_code
				and t9.create_time::date=$1
		)
;

--临时删除暂停营业店铺
--delete from rst.rst_product_onsale where shop_code='080899';

--插入到历史表
delete from rst.rst_product_onsale_history where sale_date=$1;
insert into rst.rst_product_onsale_history 
(
	shop_code,
	shop_name,
	product_code,
	product_name,
	product_type,
	sale_date,
	price_sale,
	price_wholesale,
	shelf_life,
	measure_unit,
	min_qty,
	min_display_qty,
	best_life,
	is_main
) 
select 
	shop_code,
	shop_name,
	product_code,
	product_name,
	product_type,
	$1,
	price_sale,
	price_wholesale,
	shelf_life,
	measure_unit,
	min_qty,
	min_display_qty,
	best_life,
	is_main
from rst.rst_product_onsale
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

	RETURN 1;
END

$function$
;

```