# product_target_moniitor
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_target_monitor(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN
	
--rst.rst_product_target_monitor
--============================================================================================
--每天每家门店每种商品销售额、进货额合计
drop table if exists tmp1;
create temporary table tmp1 as 
select t1.shop_code,
	   t1.product_code,
	   t1.sale_date,
	   sum(t1.qty*coalesce(t2.price_sale,0)) sale_amt,
	   sum(t1.qty*coalesce(t2.price_wholesale,0)) wholesale_amt
from edw.fct_pos_detail t1
left join edw.dim_price t2 
	on t1.shop_code=t2.shop_code 
	and t1.product_code=t2.product_code
where t1.sale_date=$1
group by t1.shop_code,
		 t1.product_code,
		 t1.sale_date
;

--每天每家门店每种商品报废金额合计
drop table if exists tmp2;
create temporary table tmp2 as 
select t3.shop_code,
	   t3.product_code,
	   t3.loss_date sale_date,
	   sum(t3.qty*coalesce(t4.price_sale,0)) loss_amt
from edw.fct_loss t3 
left join edw.dim_price t4 
	on t3.shop_code=t4.shop_code 
	and t3.product_code=t4.product_code
where t3.loss_date=$1 
group by t3.shop_code,
		 t3.product_code,
		 t3.loss_date 
;

--订单筛选出每天不重复订单
drop table if exists tmp_a;
create temporary table tmp_a as 
select 
	distinct doc_no,
	shop_code,
	sale_date,
	sale_time,
	remark
from edw.fct_pos 
where sale_date=$1
;

--每天每家门店每种商品品尝金额合计
drop table if exists tmp3;
create temporary table tmp3 as 
select t6.shop_code,
	   t6.product_code,
	   t6.sale_date,
	   sum(t6.qty*coalesce(t7.price_sale,0)) taste_amt
from tmp_a t5
left join edw.fct_pos_detail t6 
	on t5.doc_no=t6.doc_no
	and t5.shop_code=t6.shop_code
	and t5.sale_date=t6.sale_date
left join edw.dim_price t7 
	on t6.shop_code=t7.shop_code 
	and t6.product_code=t7.product_code 
where t5.remark='品尝品' 
	and t6.amt=0 
	and t6.sale_date=$1
group by t6.shop_code,
		 t6.product_code,
		 t6.sale_date
;

--每天每家门店每种商品赠送金额合计
drop table if exists tmp4;
create temporary table tmp4 as 
select t9.shop_code,
	   t9.product_code,
	   t9.sale_date,
	   sum(t9.qty*coalesce(t10.price_sale,0)) give_amt
from tmp_a t8
left join edw.fct_pos_detail t9 
	on t8.doc_no=t9.doc_no
	and t8.shop_code=t9.shop_code
	and t8.sale_date=t9.sale_date
left join edw.dim_price t10 
	on t9.shop_code=t10.shop_code 
	and t9.product_code=t10.product_code
where t8.remark<>'品尝品' 
	and t9.amt=0 
	and t9.sale_date=$1
group by t9.shop_code,
		 t9.product_code,
		 t9.sale_date
;

--每天每家门店每笔订单临期折价金额
drop table if exists tmp5;
create temporary table tmp5 as 
select shop_code,
	   sale_date,
	   doc_no,
	   sum(-amt) discount_amt
from edw.fct_pos_detail 
where product_code in ('2015003','2015004','2015005') 
	and sale_date=$1 
group by shop_code,
		 sale_date,
		 doc_no
;

--每笔订单按金额进行排序，取第一个商品作为折扣商品
drop table if exists tmp6;
create temporary table tmp6 as 
select row_number() over(partition by t12.doc_no,t12.shop_code,t12.sale_date order by t12.amt desc) product_id,
	   t12.shop_code,
	   t12.product_code,
	   t12.sale_date,
	   t12.doc_no,
	   t11.discount_amt
from tmp5 t11
left join edw.fct_pos_detail t12 
	on t11.doc_no=t12.doc_no
	and t11.shop_code=t12.shop_code
	and t11.sale_date=t12.sale_date
where t12.product_code not in ('2015003','2015004','2015005') 
;

--每天每家门店每个商品临期折价金额合计
drop table if exists tmp7;
create temporary table tmp7 as 
select shop_code,
	   product_code,
	   sale_date,
	   sum(discount_amt) discount_amt
from tmp6
where product_id=1
group by shop_code,
		 product_code,
		 sale_date
;

--合并数据
drop table if exists tmp8;
create temporary table tmp8 as 
select coalesce(t13.shop_code,t14.shop_code) shop_code,
	   coalesce(t13.product_code,t14.product_code) product_code,
	   coalesce(t13.sale_date,t14.sale_date) sale_date,
	   coalesce(t13.sale_amt,0) sale_amt,
	   coalesce(t13.wholesale_amt,0) wholesale_amt,
	   coalesce(t14.loss_amt,0) loss_amt,
	   coalesce(t15.taste_amt,0) taste_amt,
	   coalesce(t16.give_amt,0) give_amt,
	   coalesce(t17.discount_amt,0) discount_amt,
	   coalesce(t14.loss_amt,0)+coalesce(t15.taste_amt,0)+coalesce(t16.give_amt,0)+coalesce(t17.discount_amt,0) sum_cost_amt
from tmp1 t13
full join tmp2 t14 
	on t13.shop_code=t14.shop_code 
	and t13.product_code=t14.product_code 
	and t13.sale_date=t14.sale_date
left join tmp3 t15 
	on t13.shop_code=t15.shop_code 
	and t13.product_code=t15.product_code 
	and t13.sale_date=t15.sale_date
left join tmp4 t16 
	on t13.shop_code=t16.shop_code 
	and t13.product_code=t16.product_code 
	and t13.sale_date=t16.sale_date
left join tmp7 t17 
	on t13.shop_code=t17.shop_code 
	and t13.product_code=t17.product_code 
	and t13.sale_date=t17.sale_date
;

--插入数据
delete from rst.rst_product_target_monitor where sale_date=$1;
insert into rst.rst_product_target_monitor 
(
	   shop_code,
	   shop_name,
	   product_type,
	   product_code,
	   product_name,
	   sale_date,
	   sale_amt,
	   wholesale_amt,
	   loss_amt,
	   taste_amt,
	   give_amt,
	   discount_amt,
	   sum_cost_amt,
	   customer_qty,
	   customer_price
) 
select 
	   t18.shop_code,
	   t19.shop_name,
	   t20.product_type,
	   t18.product_code,
	   t20.product_name,
	   t18.sale_date,
	   t18.sale_amt,
	   t18.wholesale_amt,
	   t18.loss_amt,
	   t18.taste_amt,
	   t18.give_amt,
	   t18.discount_amt,
	   t18.sum_cost_amt,
	   t21.customer_qty,
	   t21.customer_price
from tmp8 t18
left join edw.dim_shop t19
	on t18.shop_code=t19.shop_code
left join edw.dim_product t20
	on t18.product_code=t20.product_code
left join rst.rst_management_analysis t21
	on t18.shop_code=t21.shop_code
	and t18.sale_date=t21.sale_date
where t18.sale_date=$1 
	and t20.product_type is not null
;


--rst.rst_product_target_monitor_week
--============================================================================================
--按周汇总客单数、客单价
drop table if exists tmp9_a;
create temporary table tmp9_a as 
select 
	t3.shop_code,
	t4.shop_name,
	t5.product_type,
	t3.product_code,
	t5.product_name,
	t2.sale_year,
	t2.sale_week,
	sum(t3.sale_amt) sale_amt,
	sum(t3.wholesale_amt) wholesale_amt,
	sum(t3.loss_amt) loss_amt,
	sum(t3.taste_amt) taste_amt,
	sum(t3.give_amt) give_amt,
	sum(t3.discount_amt) discount_amt,
	sum(t3.sum_cost_amt) sum_cost_amt
from rst.rst_week t1
left join rst.rst_week t2
	on t1.sale_year=t2.sale_year
	and t1.sale_week=t2.sale_week
inner join rst.rst_product_target_monitor t3
	on t2.sale_date=t3.sale_date
left join rst.rst_shop t4 
	on t3.shop_code=t4.shop_code
left join rst.rst_product t5 
	on t3.product_code=t5.product_code
where t1.sale_date=$1
group by 
	t3.shop_code,
	t4.shop_name,
	t5.product_type,
	t3.product_code,
	t5.product_name,
	t2.sale_year,
	t2.sale_week
;

drop table if exists tmp9_b;
create temporary table tmp9_b as 
select 
	t3.shop_code,
	t2.sale_year,
	t2.sale_week,
	sum(t3.customer_qty) customer_qty,
	sum(t3.sale_amt)/sum(t3.customer_qty) customer_price
from rst.rst_week t1
left join rst.rst_week t2
	on t1.sale_year=t2.sale_year
	and t1.sale_week=t2.sale_week
inner join rst.rst_management_analysis t3
	on t2.sale_date=t3.sale_date
where t1.sale_date=$1
group by 
	t3.shop_code,
	t2.sale_year,
	t2.sale_week
;

--插入数据
delete from rst.rst_product_target_monitor_week t1
using rst.rst_week t2
where t1.sale_year=t2.sale_year
	and t1.sale_week=t2.sale_week
	and t2.sale_date=$1
;
insert into rst.rst_product_target_monitor_week
(
	shop_code,
	shop_name,
	product_type,
	product_code,
	product_name,
	sale_year,
	sale_week,
	sale_amt,
	wholesale_amt,
	loss_amt,
	taste_amt,
	give_amt,
	discount_amt,
	sum_cost_amt,
	customer_qty,
	customer_price
) 
select 
	t22.shop_code,
	t22.shop_name,
	t22.product_type,
	t22.product_code,
	t22.product_name,
	t22.sale_year,
	t22.sale_week,
	t22.sale_amt,
	t22.wholesale_amt,
	t22.loss_amt,
	t22.taste_amt,
	t22.give_amt,
	t22.discount_amt,
	t22.sum_cost_amt,
	t23.customer_qty,
	t23.customer_price
from tmp9_a t22
left join tmp9_b t23
	on t22.shop_code=t23.shop_code
	and t22.sale_year=t23.sale_year
	and t22.sale_week=t23.sale_week
;


--rst.rst_product_target_monitor_month
--============================================================================================
--按月汇总客单数、客单价
drop table if exists tmp10;
create temporary table tmp10 as 
select 
	shop_code,
	extract(year from sale_date) sale_year,
	extract(month from sale_date) sale_month,
	sum(customer_qty) customer_qty,
	sum(sale_amt)/sum(customer_qty) customer_price
from rst.rst_management_analysis 
where extract(year from sale_date)=extract(year from $1)
	and extract(month from sale_date)=extract(month from $1)
group by 
	shop_code,
	extract(year from sale_date),
	extract(month from sale_date)
;

--插入数据
delete from rst.rst_product_target_monitor_month 
where sale_year=extract(year from $1) 
	and sale_month=extract(month from $1)
;
insert into rst.rst_product_target_monitor_month
(
	shop_code,
	shop_name,
	product_type,
	product_code,
	product_name,
	sale_year,
	sale_month,
	sale_amt,
	wholesale_amt,
	loss_amt,
	taste_amt,
	give_amt,
	discount_amt,
	sum_cost_amt,
	customer_qty,
	customer_price
) 
select 
	t24.shop_code,
	t26.shop_name,
	t27.product_type,
	t24.product_code,
	t27.product_name,
	extract(year from t24.sale_date) sale_year,
	extract(month from t24.sale_date) sale_month,
	sum(t24.sale_amt) sale_amt,
	sum(t24.wholesale_amt) wholesale_amt,
	sum(t24.loss_amt) loss_amt,
	sum(t24.taste_amt) taste_amt,
	sum(t24.give_amt) give_amt,
	sum(t24.discount_amt) discount_amt,
	sum(t24.sum_cost_amt) sum_cost_amt,
	t25.customer_qty,
	t25.customer_price
from rst.rst_product_target_monitor t24
left join tmp10 t25
	on t24.shop_code=t25.shop_code
	and extract(year from t24.sale_date)=t25.sale_year
	and extract(month from t24.sale_date)=t25.sale_month
left join rst.rst_shop t26
	on t24.shop_code=t26.shop_code
left join rst.rst_product t27 
	on t24.product_code=t27.product_code
where extract(year from t24.sale_date)=extract(year from $1)
	and extract(month from t24.sale_date)=extract(month from $1)
group by 
	t24.shop_code,
	t26.shop_name,
	t27.product_type,
	t24.product_code,
	t27.product_name,
	extract(year from t24.sale_date),
	extract(month from t24.sale_date),
	t25.customer_qty,
	t25.customer_price
;


--rst.rst_product_target_monitor_year
--============================================================================================
--按年汇总客单数、客单价
drop table if exists tmp11;
create temporary table tmp11 as 
select 
	shop_code,
	extract(year from sale_date) sale_year,
	sum(customer_qty) customer_qty,
	sum(sale_amt)/sum(customer_qty) customer_price
from rst.rst_management_analysis 
where extract(year from sale_date)=extract(year from $1)
group by 
	shop_code,
	extract(year from sale_date)
;

--插入数据
delete from rst.rst_product_target_monitor_year 
where sale_year=extract(year from $1)
;
insert into rst.rst_product_target_monitor_year
(
	shop_code,
	shop_name,
	product_type,
	product_code,
	product_name,
	sale_year,
	sale_amt,
	wholesale_amt,
	loss_amt,
	taste_amt,
	give_amt,
	discount_amt,
	sum_cost_amt,
	customer_qty,
	customer_price
) 
select 
	t26.shop_code,
	t28.shop_name,
	t29.product_type,
	t26.product_code,
	t29.product_name,
	extract(year from t26.sale_date) sale_year,
	sum(t26.sale_amt) sale_amt,
	sum(t26.wholesale_amt) wholesale_amt,
	sum(t26.loss_amt) loss_amt,
	sum(t26.taste_amt) taste_amt,
	sum(t26.give_amt) give_amt,
	sum(t26.discount_amt) discount_amt,
	sum(t26.sum_cost_amt) sum_cost_amt,
	t27.customer_qty,
	t27.customer_price
from rst.rst_product_target_monitor t26
left join tmp11 t27 
	on t26.shop_code=t27.shop_code
	and extract(year from t26.sale_date)=t27.sale_year
left join rst.rst_shop t28
	on t26.shop_code=t28.shop_code
left join rst.rst_product t29
	on t26.product_code=t29.product_code
where extract(year from t26.sale_date)=extract(year from $1)
group by 
	t26.shop_code,
	t28.shop_name,
	t29.product_type,
	t26.product_code,
	t29.product_name,
	extract(year from t26.sale_date),
	t27.customer_qty,
	t27.customer_price
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
drop table if exists tmp_a;

	RETURN 1;
END

$function$
;

```