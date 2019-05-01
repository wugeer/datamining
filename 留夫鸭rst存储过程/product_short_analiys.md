# product_short_analiys
```
CREATE OR REPLACE FUNCTION rst.p_rst_product_short_analysis(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

--前一日实际销量
drop table if exists tmp1;
create temporary table tmp1 as 
select 
	t1.shop_code,
	t1.product_code,
	t1.sale_date,
	sum(t1.qty) actual_sale_qty
from rst.rst_pos t1
where t1.sale_date=$1-1
	and t1.amt<>0
group by
	t1.shop_code,
	t1.product_code,
	t1.sale_date
;


--前一日报废
drop table if exists tmp2;
create temporary table tmp2 as 
select 
	t1.shop_code,
	t1.product_code,
	t1.loss_date,
	sum(t1.qty) loss_qty
from edw.fct_loss t1
where t1.loss_date=$1-1
group by 
	t1.shop_code,
	t1.product_code,
	t1.loss_date
;


--订单筛选出前一日不重复订单
drop table if exists tmp3;
create temporary table tmp3 as 
select 
	distinct t1.doc_no,
	t1.shop_code,
	t1.sale_date,
	remark
from edw.fct_pos t1 
where t1.sale_date=$1-1
;


--前一日品尝
drop table if exists tmp4;
create temporary table tmp4 as 
select 
	t1.shop_code,
	t1.product_code,
	t1.sale_date,
	sum(t1.qty) taste_qty
from rst.rst_pos t1 
inner join tmp3 t2
	on t1.doc_no=t2.doc_no
	and t1.shop_code=t2.shop_code
	and t1.sale_date=t2.sale_date
where t1.sale_date=$1-1
	and t1.amt=0
	and t2.remark='品尝品'
group by 
	t1.shop_code,
	t1.product_code,
	t1.sale_date
;


--前一日赠送
drop table if exists tmp5;
create temporary table tmp5 as 
select 
	t1.shop_code,
	t1.product_code,
	t1.sale_date,
	sum(t1.qty) give_qty
from rst.rst_pos t1 
inner join tmp3 t2
	on t1.doc_no=t2.doc_no
	and t1.shop_code=t2.shop_code
	and t1.sale_date=t2.sale_date
where t1.sale_date=$1-1
	and t1.amt=0
	and t2.remark<>'品尝品'
group by 
	t1.shop_code,
	t1.product_code,
	t1.sale_date
;


--前一日调拨入
drop table if exists tmp6;
create temporary table tmp6 as 
select 
	t1.shop_code_in shop_code,
	t1.product_code,
	t1.allot_date,
	sum(t1.qty) allot_in_qty
from edw.fct_allot t1 
where t1.allot_date=$1-1
group by 
	t1.shop_code_in,
	t1.product_code,
	t1.allot_date
;


--前一日调拨出
drop table if exists tmp7;
create temporary table tmp7 as 
select 
	t1.shop_code_out shop_code,
	t1.product_code,
	t1.allot_date,
	sum(t1.qty) allot_out_qty
from edw.fct_allot t1 
where t1.allot_date=$1-1
group by 
	t1.shop_code_out,
	t1.product_code,
	t1.allot_date
;


--计算前一日日末库存
drop table if exists tmp8;
create temporary table tmp8 as 
select 
	t1.shop_code,
	t1.product_code,
	t1.bill_date,
	coalesce(t2.d0_end_qty,0)+coalesce(t2.d1_in_qty,0)-coalesce(t3.actual_sale_qty,0)-coalesce(t4.loss_qty,0)-coalesce(t5.taste_qty,0)-coalesce(t6.give_qty,0)+coalesce(t7.allot_in_qty,0)-coalesce(t8.allot_out_qty,0) computer_end_qty 
from rst.rst_product_bill_shop t1 
left join rst.rst_product_bill_shop t2
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
	and t1.bill_date=t2.bill_date+1
left join tmp1 t3 
	on t1.shop_code=t3.shop_code
	and t1.product_code=t3.product_code
	and t1.bill_date=t3.sale_date+1
left join tmp2 t4
	on t1.shop_code=t4.shop_code
	and t1.product_code=t4.product_code
	and t1.bill_date=t4.loss_date+1
left join tmp4 t5 
	on t1.shop_code=t5.shop_code
	and t1.product_code=t5.product_code
	and t1.bill_date=t5.sale_date+1
left join tmp5 t6 
	on t1.shop_code=t6.shop_code
	and t1.product_code=t6.product_code
	and t1.bill_date=t6.sale_date+1
left join tmp6 t7 
	on t1.shop_code=t7.shop_code
	and t1.product_code=t7.product_code
	and t1.bill_date=t7.allot_date+1
left join tmp7 t8 
	on t1.shop_code=t8.shop_code
	and t1.product_code=t8.product_code
	and t1.bill_date=t8.allot_date+1
where t1.bill_date=$1
;


--更新前三天数据
drop table if exists tmp9;
create temporary table tmp9 as 
select 
	t1.shop_code,
	t1.shop_name,
	t1.bill_date,
	t1.arrive_date,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t1.shorts_time,
	t1.check_status,
	t1.computer_end_qty,
	t1.d0_end_qty,
	t1.d1_in_qty,
	t1.d2_in_qty,
	t1.d2_end_qty_predict,
	t1.optimal_purchase,
	t1.final_purchase,
	t1.d3_sale_predict,
	t1.actual_sale_qty,
	t1.loss_qty,
	t1.taste_qty,
	t1.give_qty,
	t1.allot_qty,
	t2.actual_sale_qty d3_sale_actual
from rst.rst_product_short_analysis t1
left join tmp1 t2
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
	and t1.arrive_date=t2.sale_date
where t1.bill_date=$1-3
;


--插入前三天数据
delete from rst.rst_product_short_analysis where bill_date=$1-3;
insert into rst.rst_product_short_analysis
(
	shop_code,
	shop_name,
	bill_date,
	arrive_date,
	product_type,
	product_code,
	product_name,
	shorts_time,
	check_status,
	computer_end_qty,
	d0_end_qty,
	d1_in_qty,
	d2_in_qty,
	d2_end_qty_predict,
	optimal_purchase,
	final_purchase,
	d3_sale_predict,
	actual_sale_qty,
	loss_qty,
	taste_qty,
	give_qty,
	allot_qty,
	d3_sale_actual
)
select 
	shop_code,
	shop_name,
	bill_date,
	arrive_date,
	product_type,
	product_code,
	product_name,
	shorts_time,
	check_status,
	computer_end_qty,
	d0_end_qty,
	d1_in_qty,
	d2_in_qty,
	d2_end_qty_predict,
	optimal_purchase,
	final_purchase,
	d3_sale_predict,
	actual_sale_qty,
	loss_qty,
	taste_qty,
	give_qty,
	allot_qty,
	d3_sale_actual
from tmp9
;


--更新前一天数据
drop table if exists tmp10;
create temporary table tmp10 as 
select 
	t1.shop_code,
	t1.shop_name,
	t1.bill_date,
	t1.arrive_date,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	t2.shorts_time,
	t1.check_status,
	t1.computer_end_qty,
	t1.d0_end_qty,
	t1.d1_in_qty,
	t1.d2_in_qty,
	t1.d2_end_qty_predict,
	t1.optimal_purchase,
	t9.final_purchase,
	t1.d3_sale_predict,
	t3.actual_sale_qty,
	t4.loss_qty,
	t5.taste_qty,
	t6.give_qty,
	case 
		when coalesce(t7.allot_in_qty,0)-coalesce(t8.allot_out_qty,0)=0 then null 
		else coalesce(t7.allot_in_qty,0)-coalesce(t8.allot_out_qty,0)
	end allot_qty,
	t1.d3_sale_actual
from rst.rst_product_short_analysis t1 
left join analysis.analysis_shorts_detail t2
	on t1.shop_code=t2.shop_code
	and t1.product_code=t2.product_code
	and t1.bill_date=t2.sale_date
left join tmp1 t3 
	on t1.shop_code=t3.shop_code
	and t1.product_code=t3.product_code
	and t1.bill_date=t3.sale_date
left join tmp2 t4
	on t1.shop_code=t4.shop_code
	and t1.product_code=t4.product_code
	and t1.bill_date=t4.loss_date
left join tmp4 t5 
	on t1.shop_code=t5.shop_code
	and t1.product_code=t5.product_code
	and t1.bill_date=t5.sale_date
left join tmp5 t6 
	on t1.shop_code=t6.shop_code
	and t1.product_code=t6.product_code
	and t1.bill_date=t6.sale_date
left join tmp6 t7 
	on t1.shop_code=t7.shop_code
	and t1.product_code=t7.product_code
	and t1.bill_date=t7.allot_date
left join tmp7 t8 
	on t1.shop_code=t8.shop_code
	and t1.product_code=t8.product_code
	and t1.bill_date=t8.allot_date
left join rst.rst_product_bill_shop t9
	on t1.shop_code=t9.shop_code
	and t1.product_code=t9.product_code
	and t1.bill_date=t9.bill_date
where t1.bill_date=$1-1
;


--插入前一天数据
delete from rst.rst_product_short_analysis where bill_date=$1-1;
insert into rst.rst_product_short_analysis
(
	shop_code,
	shop_name,
	bill_date,
	arrive_date,
	product_type,
	product_code,
	product_name,
	shorts_time,
	check_status,
	computer_end_qty,
	d0_end_qty,
	d1_in_qty,
	d2_in_qty,
	d2_end_qty_predict,
	optimal_purchase,
	final_purchase,
	d3_sale_predict,
	actual_sale_qty,
	loss_qty,
	taste_qty,
	give_qty,
	allot_qty,
	d3_sale_actual
)
select 
	shop_code,
	shop_name,
	bill_date,
	arrive_date,
	product_type,
	product_code,
	product_name,
	shorts_time,
	check_status,
	computer_end_qty,
	d0_end_qty,
	d1_in_qty,
	d2_in_qty,
	d2_end_qty_predict,
	optimal_purchase,
	final_purchase,
	d3_sale_predict,
	actual_sale_qty,
	loss_qty,
	taste_qty,
	give_qty,
	allot_qty,
	d3_sale_actual
from tmp10
;


--插入当天数据
delete from rst.rst_product_short_analysis where bill_date=$1;
insert into rst.rst_product_short_analysis
(
	shop_code,
	shop_name,
	bill_date,
	arrive_date,
	product_type,
	product_code,
	product_name,
	shorts_time,
	check_status,
	computer_end_qty,
	d0_end_qty,
	d1_in_qty,
	d2_in_qty,
	d2_end_qty_predict,
	optimal_purchase,
	final_purchase,
	d3_sale_predict,
	actual_sale_qty,
	loss_qty,
	taste_qty,
	give_qty,
	allot_qty,
	d3_sale_actual
)
select 
	t1.shop_code,
	t1.shop_name,
	t1.bill_date,
	t1.arrive_date,
	t1.product_type,
	t1.product_code,
	t1.product_name,
	null shorts_time,
	case 
		when coalesce(t1.d0_end_qty,0)=coalesce(t9.computer_end_qty,0) then null 
		else '盘点异常'
	end check_status,
	t9.computer_end_qty,
	t1.d0_end_qty,
	t1.d1_in_qty,
	t1.d2_in_qty,
	t1.d2_end_qty_predict,
	t1.optimal_purchase,
	t1.final_purchase,
	t1.d3_sale_predict,
	null actual_sale_qty,
	null loss_qty,
	null taste_qty,
	null give_qty,
	null allot_qty,
	null d3_sale_actual
from rst.rst_product_bill_shop t1 
left join tmp8 t9
	on t1.shop_code=t9.shop_code
	and t1.product_code=t9.product_code
	and t1.bill_date=t9.bill_date
where t1.bill_date=$1
	and t1.product_code not like '02%'
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

	RETURN 1;
END

$function$
;

```