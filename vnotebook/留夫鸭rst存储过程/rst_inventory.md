# rst_inventory
```
CREATE OR REPLACE FUNCTION rst.p_rst_inventory(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--取当天在售商品
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	distinct shop_code,
	product_code,
	$1 check_date
from rst.rst_product_onsale
;
--select * from tmp1 

/*
--补全日期
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	shop_code,
	product_code,
	check_date
from tmp1
cross join (select generate_series('2016-07-01'::date,'2018-07-10'::date,'1 day') check_date) b 
;
*/

--期初库存
drop table if exists tmp3;
create temporary table tmp3 as 
select 
	shop_code,
	product_code,
	qty,
	(check_date+'1 day'::interval)::date check_date
from rst.rst_stock
where check_date=($1-'1 day'::interval)::date
;

--入库
drop table if exists tmp4;
create temporary table tmp4 as 
select 
	shop_code,
	product_code,
	sum(qty) qty,
	sum(amt) amt,
	in_date
from edw.fct_input
where in_date=$1
group by 
	shop_code,
	product_code,
	in_date
;

--出库
drop table if exists tmp5;
create temporary table tmp5 as 
select 
	shop_code,
	product_code,
	sum(qty) qty,
	sum(amt) amt,
	sale_date
from edw.fct_pos_detail
where sale_date=$1
group by 
	shop_code,
	product_code,
	sale_date
;

--报损
drop table if exists tmp6;
create temporary table tmp6 as 
select 
	shop_code,
	product_code,
	sum(qty) qty,
	sum(amt) amt,
	loss_date
from edw.fct_loss
where loss_date=$1
group by 
	shop_code,
	product_code,
	loss_date
;

--团购
drop table if exists tmp14;
create temporary table tmp14 as 
select 
	shop_code,
	product_code,
	sum(fin_qty) qty,
	bill_date
from rst.rst_product_bill_other
where bill_date=$1
	and goods_type='团购要货'
group by 
	shop_code,
	product_code,
	bill_date
;

--品尝赠送
drop table if exists tmp15;
create temporary table tmp15 as 
select 
	shop_code,
	product_code,
	sale_date,
	sum(qty) taste_give_qty
from edw.fct_pos_detail 
where sale_date=$1
	and amt=0
group by 
	shop_code,
	product_code,
	sale_date
;

--合并
drop table if exists tmp7;
create temporary table tmp7 as 
select 
	t2.shop_code,
	t2.product_code,
	t2.check_date,
	t3.qty begin_qty,
	t4.qty in_qty,
	t15.qty group_buying_qty,
	t5.qty sale_qty,
	t6.qty loss_qty,
	t7.qty end_qty,
	t16.taste_give_qty
from tmp1 t2
left join tmp3 t3 
	on t2.shop_code=t3.shop_code 
	and t2.product_code=t3.product_code 
	and t2.check_date=t3.check_date
left join tmp4 t4 
	on t2.shop_code=t4.shop_code 
	and t2.product_code=t4.product_code 
	and t2.check_date=t4.in_date
left join tmp14 t15 
	on t2.shop_code=t15.shop_code 
	and t2.product_code=t15.product_code 
	and t2.check_date=t15.bill_date
left join tmp5 t5 
	on t2.shop_code=t5.shop_code 
	and t2.product_code=t5.product_code 
	and t2.check_date=t5.sale_date
left join tmp6 t6 
	on t2.shop_code=t6.shop_code 
	and t2.product_code=t6.product_code 
	and t2.check_date=t6.loss_date
left join edw.fct_stock t7 
	on t2.shop_code=t7.shop_code 
	and t2.product_code=t7.product_code 
	and t2.check_date=t7.check_date
left join tmp15 t16 
	on t2.shop_code=t16.shop_code 
	and t2.product_code=t16.product_code 
	and t2.check_date=t16.sale_date
;


--取前一天倒推库存
drop table if exists tmp10;
create temporary table tmp10 as 
select 
	shop_code,
	product_code,
	check_date,
	inferential_end_qty
from rst.rst_inventory
where check_date=($1-'1 day'::interval)::date
;


--合计当天调拨出库
drop table if exists tmp11;
create temporary table tmp11 as 
select 
	shop_code_out shop_code,
	product_code,
	allot_date,
	sum(qty) qty
from edw.fct_allot
where allot_date=$1
group by 
	shop_code_out,
	product_code,
	allot_date
;


--合计当天调拨入库
drop table if exists tmp12;
create temporary table tmp12 as 
select 
	shop_code_in shop_code,
	product_code,
	allot_date,
	sum(qty) qty
from edw.fct_allot
where allot_date=$1
group by 
	shop_code_in,
	product_code,
	allot_date
;


--计算倒推库存
drop table if exists tmp13;
create temporary table tmp13 as 
select 
	t11.shop_code,
	t11.product_code,
	t11.check_date,
	t11.begin_qty,
	t11.in_qty,
	t11.group_buying_qty,
	t11.sale_qty,
	t11.loss_qty,
	t11.end_qty,
	case when t15.product_type='拌菜产品' or t16.shop_code is not null
			then coalesce(t11.end_qty,0)
		when t15.product_type<>'拌菜产品' and t16.shop_code is null and t11.end_qty is not null 
			then t11.end_qty
		when t15.product_type<>'拌菜产品' and t16.shop_code is null and t11.end_qty is null and coalesce(t14.inferential_end_qty,0)+coalesce(t11.in_qty,0)+coalesce(t13.qty,0)-coalesce(t11.loss_qty,0)-coalesce(t11.sale_qty,0)-coalesce(t12.qty,0)>=0 
			then coalesce(t14.inferential_end_qty,0)+coalesce(t11.in_qty,0)+coalesce(t13.qty,0)-coalesce(t11.loss_qty,0)-coalesce(t11.sale_qty,0)-coalesce(t12.qty,0)
		else 0
	end inferential_end_qty,
	t11.taste_give_qty
from tmp7 t11 
left join tmp11 t12 
	on t11.shop_code=t12.shop_code 
	and t11.product_code=t12.product_code 
	and t11.check_date=t12.allot_date
left join tmp12 t13 
	on t11.shop_code=t13.shop_code
	and t11.product_code=t13.product_code 
	and t11.check_date=t13.allot_date
left join tmp10 t14 
	on t11.shop_code=t14.shop_code 
	and t11.product_code=t14.product_code 
	and t11.check_date=(t14.check_date+'1 day'::interval)::date
left join rst.rst_product t15 
	on t11.product_code=t15.product_code
left join rst.rst_shop_pos_inaccurate t16
	on t11.shop_code=t16.shop_code
where t11.check_date=$1
;


--插入数据
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
	loss_qty,
	end_qty,
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
	loss_qty,
	end_qty,
	inferential_end_qty,
	taste_give_qty
from tmp13
;

/*
--找出连续两次有入库或销售或库存记录的日期（用来删除历史）
drop table if exists tmp8;
create temporary table tmp8 as 
select shop_code,
	product_code,
	check_date,
	lead(check_date,1,'2016-06-30'::date) over(partition by shop_code,product_code order by check_date desc) before_date
from rst.rst_inventory
where (in_qty is not null or sale_qty is not null or end_qty is not null)
;

--找出两个日期间隔大于7天的商品
drop table if exists tmp9;
create temporary table tmp9 as 
select shop_code,
	product_code,
	check_date,
	before_date
from tmp8
where check_date-before_date>='8 day'::interval
;

--删除连续7天以上没有入库或销售或库存记录的商品
delete from rst.rst_inventory t8
using tmp9 t9
where (t8.check_date<t9.check_date and t8.check_date>t9.before_date)
	and t8.shop_code=t9.shop_code and t8.product_code=t9.product_code
	and (t8.in_qty is null and t8.sale_qty is null and t8.end_qty is null)
;
*/

--找出上一次有入库或销售或库存记录的日期（用来删除增量）
drop table if exists tmp8;
create temporary table tmp8 as 
select 
	t1.shop_code,
	t1.product_code,
	$1 check_date,
	max(t1.check_date::timestamp) before_date
from rst.rst_inventory t1
where t1.check_date<=$1
	and (t1.in_qty is not null or t1.sale_qty is not null or t1.inferential_end_qty<>0) 
	and not exists(select 1 from rst.rst_shop_pause_history t2
					where t1.shop_code=t2.shop_code
						and $1>=t2.resume_time_point-2
						and $1<=t2.resume_time_point+7)
group by 
	t1.shop_code,
	t1.product_code,
	$1
;

--找出两个日期间隔大于7天，且不在途的商品
drop table if exists tmp9;
create temporary table tmp9 as 
select
	shop_code,
	product_code,
	check_date,
	before_date
from tmp8 a
where check_date-before_date>='7 day'::interval
	and not exists
	(
		select 1 from rst.rst_product_bill_shop b 
		where a.shop_code=b.shop_code
			and a.product_code=b.product_code
			and b.bill_date>=($1-'1 day'::interval)::date
			and b.bill_date<=$1
			and b.final_purchase<>0
	)
;

--删除inventory中连续7天以上没有入库或销售或库存记录的商品
delete from rst.rst_inventory t8
using tmp9 t9
where (t8.check_date<=t9.check_date and t8.check_date>t9.before_date)
	and t8.shop_code=t9.shop_code 
	and t8.product_code=t9.product_code
	and (t8.in_qty is null and t8.sale_qty is null and t8.inferential_end_qty=0)
;

--删除onsale中连续7天以上没有销售或库存记录的商品
delete from rst.rst_product_onsale t10
where not exists 
	(
		select 1 from rst.rst_inventory t11 
		where t10.shop_code=t11.shop_code
			and t10.product_code=t11.product_code
			and t11.check_date=$1
	)
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
drop table if exists tmp11;
drop table if exists tmp12;
drop table if exists tmp13;
drop table if exists tmp14;
drop table if exists tmp15;

	RETURN 1;
END

$function$
;

```