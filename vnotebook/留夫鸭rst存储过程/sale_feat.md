# sale_feat
```
CREATE OR REPLACE FUNCTION rst.p_rst_sale_feat(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--得到每家门店的当天在售商品，以及商品销量
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t1.shop_code,
	t2.product_code,
	t1.sale_date,
	t1.date_case,
	t1.turnover,
	t1.temp,
	t2.sale_total_qty qty
from rst.rst_turnover_feat t1
left join rst.rst_inventory t2 
	on t1.shop_code=t2.shop_code 
	and t1.sale_date=t2.check_date
where t1.sale_date=$1
;


--得到每家门店的未来七天销售商品，以及营业额
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	t3.shop_code,
	t4.product_code,
	t3.sale_date,
	t3.date_case,
	case when t3.sale_date=($1+'1 day'::interval)::date then t5.d1
			 when t3.sale_date=($1+'2 day'::interval)::date then t5.d2
			 when t3.sale_date=($1+'3 day'::interval)::date then t5.d3
			 when t3.sale_date=($1+'4 day'::interval)::date then t5.d4
			 when t3.sale_date=($1+'5 day'::interval)::date then t5.d5
			 when t3.sale_date=($1+'6 day'::interval)::date then t5.d6
			 when t3.sale_date=($1+'7 day'::interval)::date then t5.d7
	end turnover,
	t3.temp
from rst.rst_turnover_feat t3
left join rst.rst_product_onsale t4 
	on t3.shop_code=t4.shop_code
left join rst.rst_turnover_predict t5 
	on t3.shop_code=t5.shop_code 
	and t5.sale_date=($1+'1 day'::interval)::date
where t3.sale_date>=($1+'1 day'::interval)::date
	and t3.sale_date<=($1+'7 day'::interval)::date
;


--插入七天数据
delete from rst.rst_sale_feat where sale_date=$1;
insert into rst.rst_sale_feat 
(
	shop_code,
	product_code,
	sale_date,
	date_case,
	turnover,
	temp,
	qty
)
select 
	shop_code,
	product_code,
	sale_date,
	date_case,
	turnover,
	temp,
	qty
from tmp1
;

delete from rst.rst_sale_feat 
where sale_date>=($1+'1 day'::interval)::date 
	and sale_date<=($1+'7 day'::interval)::date 
;
insert into rst.rst_sale_feat 
(
	shop_code,
	product_code,
	sale_date,
	date_case,
	turnover,
	temp
)
select 
	shop_code,
	product_code,
	sale_date,
	date_case,
	turnover,
	temp
from tmp2
;


--筛选（不含当天）同case的近30天商品
drop table if exists tmp3;
create temporary table tmp3 as 
select 
	row_number() over(partition by shop_code,product_code,date_case order by sale_date desc) date_id,
	shop_code,
	product_code,
	sale_date,
	date_case,
	qty
from rst.rst_sale_feat
where sale_date<$1 
	and qty is not null
;

drop table if exists tmp4;
create temporary table tmp4 as 
select 
	shop_code,
	product_code,
	sale_date,
	date_case,
	qty
from tmp3 
where date_id<=30
;


--计算上下限
drop table if exists tmp5;
create temporary table tmp5 as 
select 
	(percentile_cont(0.25) within group(order by qty))-1.5*((percentile_cont(0.75) within group(order by qty))-(percentile_cont(0.25) within group(order by qty))) low,
	(percentile_cont(0.75) within group(order by qty))+1.5*((percentile_cont(0.75) within group(order by qty))-(percentile_cont(0.25) within group(order by qty))) high,
	shop_code,
	product_code,
	date_case
from tmp4
group by 
	shop_code,
	product_code,
	date_case
;


--剔除异常值
drop table if exists tmp6;
create temporary table tmp6 as 
select 
	t6.shop_code,
	t6.product_code,
	t6.sale_date,
	t6.date_case,
	t6.qty
from tmp4 t6
left join tmp5 t7 
	on t6.shop_code=t7.shop_code 
	and t6.product_code=t7.product_code 
	and t6.date_case=t7.date_case
where t6.qty>=t7.low 
	and t6.qty<=t7.high
;


--求出（不含当天）近三天同case商品平均销量
drop table if exists tmp7;
create temporary table tmp7 as 
select 
	row_number() over(partition by shop_code,product_code,date_case order by sale_date desc) date_id,
	shop_code,
	product_code,
	sale_date,
	date_case,
	qty
from tmp6
;

drop table if exists tmp8;
create temporary table tmp8 as 
select 
	shop_code,
	product_code,
	date_case,
	avg(qty) recent_3
from tmp7
where date_id<=3
group by 
	shop_code,
	product_code,
	date_case
;


--求出（不含当天）近七天同case商品平均销量
drop table if exists tmp9;
create temporary table tmp9 as 
select 
	shop_code,
	product_code,
	date_case,
	avg(qty) recent_7
from tmp7
where date_id<=7
group by 
	shop_code,
	product_code,
	date_case
;


--筛选同case的近30天商品
drop table if exists tmp10;
create temporary table tmp10 as 
select 
	row_number() over(partition by shop_code,product_code,date_case order by sale_date desc) date_id,
	shop_code,
	product_code,
	sale_date,
	date_case,
	qty
from rst.rst_sale_feat
where sale_date<=$1 
	and qty is not null
;

drop table if exists tmp11;
create temporary table tmp11 as 
select 
	shop_code,
	product_code,
	sale_date,
	date_case,
	qty
from tmp10 
where date_id<=30
;


--计算上下限
drop table if exists tmp12;
create temporary table tmp12 as 
select 
	(percentile_cont(0.25) within group(order by qty))-1.5*((percentile_cont(0.75) within group(order by qty))-(percentile_cont(0.25) within group(order by qty))) low,
	(percentile_cont(0.75) within group(order by qty))+1.5*((percentile_cont(0.75) within group(order by qty))-(percentile_cont(0.25) within group(order by qty))) high,
	shop_code,
	product_code,
	date_case
from tmp11
group by 
	shop_code,
	product_code,
	date_case
;


--剔除异常值
drop table if exists tmp13;
create temporary table tmp13 as 
select 
	t6.shop_code,
	t6.product_code,
	t6.sale_date,
	t6.date_case,
	t6.qty
from tmp11 t6
left join tmp12 t7 
	on t6.shop_code=t7.shop_code 
	and t6.product_code=t7.product_code 
	and t6.date_case=t7.date_case
where t6.qty>=t7.low 
	and t6.qty<=t7.high
;


--求出近三天同case商品平均销量
drop table if exists tmp14;
create temporary table tmp14 as 
select 
	row_number() over(partition by shop_code,product_code,date_case order by sale_date desc) date_id,
	shop_code,
	product_code,
	sale_date,
	date_case,
	qty
from tmp13
;

drop table if exists tmp15;
create temporary table tmp15 as 
select 
	shop_code,
	product_code,
	date_case,
	avg(qty) recent_3
from tmp14
where date_id<=3
group by 
	shop_code,
	product_code,
	date_case
;


--求出近7天同case商品平均销量
drop table if exists tmp16;
create temporary table tmp16 as 
select 
	shop_code,
	product_code,
	date_case,
	avg(qty) recent_7
from tmp14
where date_id<=7
group by 
	shop_code,
	product_code,
	date_case
;


--合并当天数据
drop table if exists tmp17;
create temporary table tmp17 as 
select 
	t8.shop_code,
	t8.product_code,
	t8.sale_date,
	t8.date_case,
	t8.turnover,
	t8.temp,
	t8.qty,
	t9.recent_3,
	t10.recent_7
from tmp1 t8 
left join tmp8 t9 
	on t8.shop_code=t9.shop_code 
	and t8.product_code=t9.product_code 
	and t8.date_case=t9.date_case
left join tmp9 t10 
	on t8.shop_code=t10.shop_code 
	and t8.product_code=t10.product_code 
	and t8.date_case=t10.date_case
;


--合并未来七天数据
drop table if exists tmp18;
create temporary table tmp18 as 
select 
	t11.shop_code,
	t11.product_code,
	t11.sale_date,
	t11.date_case,
	t11.turnover,
	t11.temp,
	t12.recent_3,
	t13.recent_7
from tmp2 t11 
left join tmp15 t12 
	on t11.shop_code=t12.shop_code 
	and t11.product_code=t12.product_code 
	and t11.date_case=t12.date_case
left join tmp16 t13 
	on t11.shop_code=t13.shop_code 
	and t11.product_code=t13.product_code 
	and t11.date_case=t13.date_case
;


--插入当天数据
delete from rst.rst_sale_feat where sale_date=$1;
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
from tmp17
;

--插入未来7天数据
delete from rst.rst_sale_feat 
where sale_date>=($1+'1 day'::interval)::date 
	and sale_date<=($1+'7 day'::interval)::date 
;
insert into rst.rst_sale_feat 
(
	shop_code,
	product_code,
	sale_date,
	date_case,
	turnover,
	temp,
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
	recent_3,
	recent_7
from tmp18
;


--有目标营业额的门店，门店销量改为空；有目标营业额的商品，单品销量改为空
update rst.rst_sale_feat t1 
set qty=null 
from rst.rst_activity t2 
where t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
	and t1.sale_date=t2.sale_date
	and t1.sale_date=$1
;


--删除rst.rst_sale_feat中7日没有销售或库存的商品
delete from rst.rst_sale_feat t14
where not exists 
(
	select 1 from rst.rst_inventory t15
	where t14.shop_code=t15.shop_code
		and t14.product_code=t15.product_code
		and t14.sale_date=t15.check_date
)
	and t14.sale_date<=$1
;

--删除rst.rst_sale_predict中7日没有销售或库存的商品
delete from rst.rst_sale_predict t16
where not exists 
(
	select 1 from rst.rst_inventory t17
	where t16.shop_code=t17.shop_code
		and t16.product_code=t17.product_code
		and t16.sale_date=t17.check_date
)
	and t16.sale_date<=$1
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
drop table if exists tmp16;
drop table if exists tmp17;
drop table if exists tmp18;

	RETURN 1;
END
$function$
;

```