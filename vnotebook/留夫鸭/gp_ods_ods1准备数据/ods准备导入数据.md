# ods准备导入数据
# 涉及type的表映射更新
```
drop table if exists test.map_product_type;
CREATE TABLE test.map_product_type (
	id serial primary key,
	product_type text NULL,
	map_columm_type text null
);
drop table if exists tmp1;
create temp table tmp1 (id serial, product_type varchar);
insert into tmp1 (product_type)
select a8 product_type
from ods.ods_oitm
union
select product_type
from ods.ods_product_bill_other
union
select product_type
from ods.ods_product_bill_shop;
truncate table test.map_product_type;
insert into test.map_product_type
(id,product_type,map_columm_type)
select id,product_type,'pr_type_'||to_char(id, 'fm0009') as map_columm_type
from tmp1;



truncate table ods1.ods_oitm;
insert into ods1.ods_oitm
(docentry,itemcode,itemname,itemlive,abc,groupcode,years,season,stdcode,msrunit,"Style",warranty,stuff,cancel,a1,a8,price,dprice,cost)
select docentry,b.map_columm_code,b.map_columm_name,itemlive,abc,groupcode,years,
season,stdcode, msrunit, "Style",warranty,stuff,cancel,a1,c.map_columm_type,price,dprice,cost
from ods.ods_oitm a
left join test.map_oitm_code_name_type b on a.itemcode=b.product_code
left join test.map_product_type c on c.product_type=a.a8
where b.map_columm_code is not null;


truncate table ods1.ods_product_bill_shop;
insert into ods1.ods_product_bill_shop
select c.map_columm_code,c.map_columm_name,bill_date,arrive_date,d.map_columm_type,b.map_columm_code,
b.map_columm_name,d0_end_qty,d1_in_qty,d2_in_qty,optimal_purchase,final_purchase,d3_sale_predict,shop_purchase,operations_purchase,feedback_reason,d2_end_qty_predict,supply_purchase 
from ods.ods_product_bill_shop a
left join test.map_product_bill_shop_product_code_name_type b on a.product_code=b.product_code
left join test.map_product_bill_shop_store_code_name c on a.shop_code=c.store_code
left join test.map_product_type d on d.product_type=a.product_type;


truncate table ods1.ods_product_bill_other;
insert into ods1.ods_product_bill_other
select goods_type,c.map_columm_code,c.map_columm_name,bill_date,arr_date,
d.map_columm_type,b.map_columm_code,b.map_columm_name,night_qty,shop_qty,area_qty,fin_qty
from ods.ods_product_bill_other a
left join test.map_product_bill_other_product_code_name_type b on a.product_code=b.product_code
left join test.map_product_bill_other_store_code_name c on c.store_code=a.shop_code
left join test.map_product_type d on d.product_type=a.product_type;



drop table if exists test.map_store_type;
create table test.map_store_type
(id serial primary key,
store_type text NULL,
map_columm_type text null);
drop table if exists tmp1;
create temp table tmp1 (id serial, store_type varchar);
insert into tmp1 (store_type)
select shptype store_type
from ods.ods_oshp
union
select cusgroup store_type
from ods.ods_ocus;
truncate table test.map_store_type;
insert into test.map_store_type
(id,store_type,map_columm_type)
select id,store_type,'st_type_'||to_char(id, 'fm0009') as map_columm_type
from tmp1;

truncate table ods1.ods_oshp;
insert into ods1.ods_oshp
select docentry,b.map_columm_code,b.map_columm_name,d.map_columm_type,shpclass,listnum,md5(address),area,province,city,cityarea,
transline,isinvalid,c.map_columm_code,empid,startdate,"cycle",
case when a.tel1 ~ '^[^0-9]*[1-9]+'
	then to_number(a.tel1,'000000000000000000000000000000000000')::varchar
	else NULL
end tel1,starttime,endtime 
from ods.ods_oshp a
left join test.map_oshp_code_name b on b.store_code=a.shpcode
left join test.map_ohem_code_name c on c.user_code=a.empdutyid
left join test.map_store_type d  on d.store_type=a.shptype;

truncate table ods1.ods_ocus;
insert into ods1.ods_ocus
select 
docentry,
b.map_columm_code,
b.map_columm_name,
c.map_columm_type,
status,
dcode,
depentry,
area,
province,
city,
cityarea,
md5(address1),
pricelist,
etl_time
from ods.ods_ocus a
left join test.map_ocus_code_name_type b on b.store_code=a.cuscode
left join test.map_store_type c on c.store_type=a.cusgroup;
```






# ods_oshp
```
drop table if exists ods1.ods_oshp;
CREATE TABLE ods1.ods_oshp (
	docentry int4 NULL,
	shpcode varchar NULL,
	shpname varchar NULL,
	shptype varchar NULL,
	shpclass varchar NULL,
	listnum int4 NULL,
	address varchar NULL,
	area varchar NULL,
	province varchar NULL,
	city varchar NULL,
	cityarea varchar NULL,
	transline varchar NULL,
	isinvalid bool NULL,
	empdutyid varchar NULL,
	empid varchar NULL,
	startdate timestamp NULL,
	"cycle" varchar NULL,
	tel1 varchar NULL,
	starttime timestamp NULL,
	endtime timestamp NULL
);
truncate table ods1.ods_oshp;
insert into ods1.ods_oshp
select docentry,b.map_columm_code,b.map_columm_name,b.map_columm_type,shpclass,listnum,md5(address),area,province,city,cityarea,
transline,isinvalid,c.map_columm_code,empid,startdate,"cycle",
case when a.tel1 ~ '^[^0-9]*[1-9]+'
	then to_number(a.tel1,'000000000000000000000000000000000000')::varchar
	else NULL
end tel1,starttime,endtime 
from ods.ods_oshp a
left join test.map_oshp_code_name b on b.store_code=a.shpcode
left join test.map_ohem_code_name c on c.user_code=a.empdutyid;
```

# ods_ohem 
```
drop table if exists ods1.ods_ohem;
CREATE TABLE ods1.ods_ohem (
	docentry int4 NULL,
	empcode varchar NULL,
	empname varchar NULL,
	dcode varchar NULL,
	wh bool NULL,
	sa bool NULL,
	pu bool NULL,
	fi bool NULL,
	dimission bool NULL,
	duty varchar NULL,
	status varchar NULL
);
truncate table ods1.ods_ohem;
insert into ods1.ods_ohem
(docentry,empcode,empname,dcode,wh,sa,pu,fi,dimission,duty,status)
select 
docentry,
b.map_columm_code,
b.map_columm_name,
dcode,
wh,
sa,
pu,
fi,
dimission,
duty,
status
from ods.ods_ohem a
left join test.map_ohem_code_name b on b.user_code=a.empcode
left join test.map_ousr_code_name c on a.empname=c.user_name;
```


# ods_customer_501 
```
drop table if exists ods1.ods_customer_501;
CREATE TABLE ods1.ods_customer_501 (
	ccuscode varchar NULL,
	ccusname varchar(98) NULL,
	ccuspperson varchar NULL
);
truncate table ods1.ods_customer_501;
insert into ods1.ods_customer_501
select b.map_columm_code,b.map_columm_name,ccuspperson 
from ods.ods_customer_501 a
left join test.map_customer_501_code_name b on a.ccuscode=b.store_code
```

# ods_customer_801
```
drop table if exists ods1.ods_customer_801;
CREATE TABLE ods1.ods_customer_801 (
	ccuscode varchar NULL,
	ccusname varchar NULL,
	ccuspperson varchar NULL
);
truncate table ods1.ods_customer_801;
insert into ods1.ods_customer_801
select b.map_columm_code,b.map_columm_name,ccuspperson
from ods.ods_customer_801 a
left join test.map_customer_801_code_name b on a.ccuscode=b.store_code;
```

# rst_shop_date_case 
```
drop table if exists ods1.rst_shop_date_case;
create table ods1.rst_shop_date_case(
	shop_code varchar NULL,
	"date" date NULL,
	holiday varchar NULL,
	week varchar NULL,
	"case" varchar NULL
);
truncate table ods1.rst_shop_date_case;
insert into ods1.rst_shop_date_case
(select b.map_columm_code,
"date",
holiday,
week,
"case"
from rst.rst_shop_date_case a
left join test.map_store_code b on a.shop_code=b.store_code)
```


# ods_product_bill_shop
```
drop table if exists ods1.ods_product_bill_shop;
CREATE TABLE ods1.ods_product_bill_shop (
	shop_code varchar NULL,
	shop_name varchar NULL,
	bill_date date NULL,
	arrive_date date NULL,
	product_type varchar NULL,
	product_code varchar NULL,
	product_name varchar NULL,
	d0_end_qty float8 NULL,
	d1_in_qty float8 NULL,
	d2_in_qty float8 NULL,
	optimal_purchase float8 NULL,
	final_purchase float8 NULL,
	d3_sale_predict float8 NULL,
	shop_purchase float8 NULL,
	operations_purchase float8 NULL,
	feedback_reason varchar NULL,
	d2_end_qty_predict float8 NULL,
	supply_purchase numeric NULL
);
truncate table ods1.ods_product_bill_shop;
insert into ods1.ods_product_bill_shop
select c.map_columm_code,c.map_columm_name,bill_date,arrive_date,b.map_columm_type,b.map_columm_code,
b.map_columm_name,d0_end_qty,d1_in_qty,d2_in_qty,optimal_purchase,final_purchase,d3_sale_predict,shop_purchase,operations_purchase,feedback_reason,d2_end_qty_predict,supply_purchase 
from ods.ods_product_bill_shop a
left join test.map_product_bill_shop_product_code_name_type b on a.product_code=b.product_code
left join test.map_product_bill_shop_store_code_name c on a.shop_code=c.store_code;
```

# ods_product_bill_other
```
drop table if exists ods1.ods_product_bill_other;
CREATE TABLE ods1.ods_product_bill_other (
	goods_type varchar NULL,
	shop_code varchar NULL,
	shop_name varchar NULL,
	bill_date date NULL,
	arr_date date NULL,
	product_type varchar NULL,
	product_code varchar NULL,
	product_name varchar NULL,
	night_qty float8 NULL,
	shop_qty float8 NULL,
	area_qty float8 NULL,
	fin_qty float8 NULL
);
truncate table ods1.ods_product_bill_other;
insert into ods1.ods_product_bill_other
select goods_type,c.map_columm_code,c.map_columm_name,bill_date,arr_date,
b.map_columm_type,b.map_columm_code,b.map_columm_name,night_qty,shop_qty,area_qty,fin_qty
from ods.ods_product_bill_other a
left join test.map_product_bill_other_product_code_name_type b on a.product_code=b.product_code
left join test.map_product_bill_other_store_code_name c on c.store_code=a.shop_code;
```

# ods_shelf_life
```
drop table if exists ods1.ods_shelf_life;
CREATE TABLE ods1.ods_shelf_life (
	shop_code varchar(20) NULL,
	product_code varchar(20) NULL,
	warranty text NULL,
	shelf_life text NULL
);
truncate table  ods1.ods_shelf_life;
insert into ods1.ods_shelf_life
select b.map_columm_code,
c.map_columm_code,
warranty,
shelf_life
from ods.ods_shelf_life a
left join test.map_store_code b on a.shop_code=b.store_code
left join test.map_product_code c on c.product_code=a.product_code;
```

# ods_target_product_turnover
```
drop table if exists ods1.ods_target_product_turnover;
CREATE TABLE ods1.ods_target_product_turnover (
	shop_code text NULL,
	product_code text NULL,
	sale_date date NULL,
	target_product_turnover float8 NULL
);
truncate table ods1.ods_target_product_turnover;
insert into ods1.ods_target_product_turnover
select b.map_columm_code,
c.map_columm_code,
sale_date,
target_product_turnover
from ods.ods_target_product_turnover a
left join test.map_store_code b on a.shop_code=b.store_code
left join test.map_product_code c on c.product_code=a.product_code;
```

# ods_display
```
drop table if exists ods1.ods_display;
CREATE TABLE ods1.ods_display (
	shop_code text NULL,
	product_code text NULL,
	min_display_qty numeric NULL
);
truncate table ods1.ods_display;
insert into ods1.ods_display
select b.map_columm_code,
c.map_columm_code,
min_display_qty
from ods.ods_display a
left join test.map_store_code b on a.shop_code=b.store_code
left join test.map_product_code c on a.product_code=c.product_code;
```


# ods_ocus
```
drop table if exists ods1.ods_ocus;
CREATE TABLE ods1.ods_ocus (
	docentry int4 NULL,
	cuscode varchar NULL,
	cusname varchar NULL,
	cusgroup varchar NULL,
	status varchar NULL,
	dcode varchar NULL,
	depentry int4 NULL,
	area varchar NULL,
	province varchar NULL,
	city varchar NULL,
	cityarea varchar NULL,
	address1 varchar NULL,
	pricelist int2 NULL,
	etl_time timestamp NULL DEFAULT now()
);
truncate table ods1.ods_ocus;
insert into ods1.ods_ocus
select 
docentry,
b.map_columm_code,
b.map_columm_name,
b.map_columm_type,
status,
dcode,
depentry,
area,
province,
city,
cityarea,
md5(address1),
pricelist,
etl_time
from ods.ods_ocus a
left join test.map_ocus_code_name_type b on b.store_code=a.cuscode;
```

# oitt
```
drop table if exists ods1.ods_oitt;
CREATE TABLE ods1.ods_oitt (
	docentry int4 NULL,
	stdcode varchar NULL,
	stdname varchar NULL,
	pcode varchar NULL,
	etl_time timestamp NULL DEFAULT now()
);
truncate table ods1.ods_oitt;
insert into ods1.ods_oitt
select docentry,b.map_columm_code,b.map_columm_name,pcode,etl_time 
from ods.ods_oitt a
left join test.map_oitt_code_name b on a.stdcode=b.product_code;
```

# msn_weather
```
drop table if exists ods1.msn_weather;
CREATE TABLE ods1.msn_weather (
	id int8 NULL,
	name text NULL,
	"date" date NULL,
	high int8 NULL,
	low int8 NULL,
	temperatures text NULL,
	wind text NULL,
	wind_dir text NULL,
	sky text NULL,
	precipitations text NULL,
	update_time timestamp NULL
);
insert into ods1.msn_weather
select id,name,"date",high,low,temperatures,wind,wind_dir,sky,precipitations,update_time from ods.msn_weather;
```

# ods_ousr
```
drop table if exists ods1.ods_ousr;
CREATE TABLE ods1.ods_ousr (
	docentry int4 NULL,
	usercode varchar NULL,
	username varchar NULL,
	froleentry int4 NULL,
	droleentry int4 NULL,
	"Admin" bool NULL,
	powergroup varchar NULL,
	datagroup varchar NULL,
	maker varchar NULL,
	createdate timestamp NULL,
	lastlogin timestamp NULL,
	lastloginout timestamp NULL,
	"Password" varchar NULL
);
truncate table ods1.ods_ousr;
insert into ods1.ods_ousr
(docentry,usercode,username,froleentry,droleentry,"Admin",powergroup,datagroup,maker,createdate,lastlogin,lastloginout,"Password") 
select docentry,b.map_columm_code,b.map_columm_name,froleentry,droleentry,"Admin",powergroup,datagroup,
b.map_column_maker,createdate,lastlogin,lastloginout,"Password" 
from ods.ods_ousr a
left join test.map_ousr_code_name b on b.user_code=a.usercode;
```



# owor
```
drop table if exists ods1.ods_owor;
CREATE TABLE ods1.ods_owor (
	docentry int4 NULL,
	docnum varchar NULL,
	whsentry int4 NULL,
	shpentry int4 NULL,
	once bool NULL,
	doctype varchar NULL,
	"Source" varchar NULL,
	cusentry int4 NULL,
	maker varchar NULL,
	audit varchar NULL,
	auditdate timestamp NULL,
	orderdate timestamp NULL,
	duedate timestamp NULL,
	docdate timestamp NULL,
	status varchar(10) NULL,
	doctotal numeric(22,4) NULL,
	paidtotal numeric(22,4) NULL,
	memo varchar NULL
);
truncate table ods1.ods_owor;
insert into ods1.ods_owor
select docentry,docnum,whsentry,shpentry,once,doctype, "Source",cusentry,md5(maker),
md5(audit),auditdate,orderdate,duedate,docdate,status,doctotal,paidtotal,memo from ods.ods_owor;
```

# oitm
```
drop table if exists ods1.ods_oitm;
CREATE TABLE ods1.ods_oitm (
	docentry int4 NULL,
	itemcode varchar NULL,
	itemname varchar NULL,
	itemlive varchar NULL,
	abc varchar NULL,
	groupcode varchar NULL,
	years varchar NULL,
	season varchar NULL,
	stdcode varchar NULL,
	msrunit varchar NULL,
	"Style" varchar NULL,
	warranty varchar NULL,
	stuff varchar NULL,
	cancel bool NULL,
	a1 varchar(50) NULL,
	a8 varchar(50) NULL,
	price numeric(21,2) NULL,
	dprice numeric(21,2) NULL,
	cost numeric(21,2) NULL
);
truncate table ods1.ods_oitm;
insert into ods1.ods_oitm
(docentry,itemcode,itemname,itemlive,abc,groupcode,years,season,stdcode,msrunit,"Style",warranty,stuff,cancel,a1,a8,price,dprice,cost)
select docentry,b.map_columm_code,b.map_columm_name,itemlive,abc,groupcode,years,
season,stdcode, msrunit, "Style",warranty,stuff,cancel,a1,b.map_columm_type,price,dprice,cost
from ods.ods_oitm a
left join test.map_oitm_code_name_type b on a.itemcode=b.product_code;
```


#  ods_sa_personuprice_501
```
drop table if exists ods1.ods_sa_personuprice_501;
CREATE TABLE ods1.ods_sa_personuprice_501 (
	cpersoncode varchar(20) NULL,
	cinvcode varchar(60) NULL,
	iinvnowcost numeric(26,6) NULL,
	dstartdate timestamp NULL,
	denddate timestamp NULL
);
truncate table ods1.ods_sa_personuprice_501;
insert into ods1.ods_sa_personuprice_501
select cpersoncode,b.map_columm_code,iinvnowcost,dstartdate,denddate
from  ods.ods_sa_personuprice_501 a
left join test.map_product_code b on a.cinvcode=b.product_code;
```

#  ods_sa_personuprice_801
```
drop table if exists ods1.ods_sa_personuprice_801;
CREATE TABLE ods1.ods_sa_personuprice_801 (
	cpersoncode varchar(20) NULL,
	cinvcode varchar(60) NULL,
	iinvnowcost numeric(26,6) NULL,
	dstartdate timestamp NULL,
	denddate timestamp NULL
);
insert into ods1.ods_sa_personuprice_801
select cpersoncode,b.map_columm_code,iinvnowcost,dstartdate,denddate
from  ods.ods_sa_personuprice_801 a
left join test.map_product_code b on a.cinvcode=b.product_code;
```


# 模型需要从gp抽到hive的表
```
truncate table ods1.rst_sale_predict;
insert into ods1.rst_sale_predict
(shop_code,
product_code,
sale_date,
qty,
d1,
d2,
d3,
d4,
d5,
d6,
d7,
error1,
error2,
error3,
error4,
error5,
error6,
error7)
select 
b.map_columm_code,
c.map_columm_code,
sale_date,
qty,
d1,
d2,
d3,
d4,
d5,
d6,
d7,
error1,
error2,
error3,
error4,
error5,
error6,
error7
from rst.rst_sale_predict a 
left join test.map_store_code_2 b on b.store_code=a.shop_code
left join test.map_product_code_2 c on c.product_code=a.product_code
where b.map_columm_code is not null and c.map_columm_code is not null;



truncate table ods1.rst_turnover_predict;
insert into ods1.rst_turnover_predict
(shop_code,
sale_date,
turnover,
d1,
d2,
d3,
d4,
d5,
d6,
d7,
mae,
r2)	
select b.map_columm_code,
sale_date,
turnover,
d1,
d2,
d3,
d4,
d5,
d6,
d7,
mae,
r2
from rst.rst_turnover_predict a 
left join test.map_store_code_2 b on a.shop_code=b.store_code
where b.map_columm_code is not null;

drop table if exists ods1.rst_product_predict_type;
CREATE TABLE ods1.rst_product_predict_type (
id serial primary key,
shop_code text NULL, -- 门店编码
product_code text NULL, -- 商品编码
new_old_type_yesterday text NULL, -- 昨日新老品类型，0为老品，1为新品
new_old_type text NULL, -- 今日新老品类型，0为老品，1为新品
predict_type text NULL, -- 预测类型，0为现有模型，1为robust
sale_date date NULL -- 日期
);
insert into ods1.rst_product_predict_type
(shop_code,
product_code,
new_old_type_yesterday ,
new_old_type,
predict_type,
sale_date)
select 
b.map_columm_code,
c.map_columm_code,
a.new_old_type_yesterday,
a.new_old_type,
a.predict_type,
a.sale_date
from rst.rst_product_predict_type a
left join test.map_store_code_2 b on a.shop_code=b.store_code
left join test.map_product_code_2 c on a.product_code=c.product_code
where b.map_columm_code is not null and c.map_columm_code is not null;


truncate table ods1.rst_product_arrive;
insert into ods1.rst_product_arrive
(shop_code,
product_code,
arrive_date,
arrive_qty,
supply_qty,
final_qty)
select b.map_columm_code,
c.map_columm_code,
a.arrive_date,
a.arrive_qty,
a.supply_qty,
a.final_qty
from rst.rst_product_arrive a 
left join test.map_store_code_2 b on a.shop_code=b.store_code
left join test.map_product_code_2 c on a.product_code=c.product_code
where b.map_columm_code is not null;


truncate table ods1.rst_product_onsale_history;
insert into ods1.rst_product_onsale_history
(shop_code,
product_code,
sale_date,
price_sale,
price_wholesale,
shelf_life,
measure_unit,
min_qty,
min_display_qty,
best_life)
select 
b.map_columm_code,
c.map_columm_code,
a.sale_date,
a.price_sale,
a.price_wholesale,
a.shelf_life,
a.measure_unit,
a.min_qty,
a.min_display_qty,
a.best_life
from rst.rst_product_onsale_history a 
left join test.map_store_code_2 b on a.shop_code=b.store_code
left join test.map_product_code_2 c on a.product_code=c.product_code
where sale_date<='2018-12-14' and c.map_columm_code is not null;

drop table if exists ods1.rst_shop_date_case;
create table ods1.rst_shop_date_case(
	shop_code varchar NULL,
	"date" date NULL,
	holiday varchar NULL,
	week varchar NULL,
	"case" varchar NULL
);
truncate table ods1.rst_shop_date_case;
insert into ods1.rst_shop_date_case
(select b.map_columm_code,
"date",
holiday,
week,
"case"
from rst.rst_shop_date_case a
left join test.map_store_code b on a.shop_code=b.store_code);
```



```
drop table if exists ods1.rst_sale_feat;
CREATE TABLE ods1.rst_sale_feat (
	shop_code varchar(20) NULL, -- 门店编码
	product_code varchar(20) NULL, -- 商品编码
	sale_date date NULL, -- 日期
	date_case varchar NULL, -- case
	turnover numeric NULL, -- 门店营业额
	temp numeric NULL, -- 温度
	qty numeric NULL, -- 数量
	recent_3 numeric NULL, -- 最近三天同case平均销量
	recent_7 numeric NULL -- 最近七天同case平均销量
);
truncate table ods1.rst_sale_feat;
insert into ods1.rst_sale_feat
(select  b.map_columm_code,
c.map_columm_code,
sale_date,
date_case,
turnover,
temp,
qty,
recent_3,
recent_7
from rst.rst_sale_feat a 
left join test.map_store_code_2 b on a.shop_code=b.store_code
left join test.map_product_code_2 c on a.product_code=c.product_code 
where a.shop_code in ('010944','081246','021339') and a.sale_date>='2018-09-03' and a.sale_date<='2018-12-14');

drop table if exists  ods1.rst_turnover_feat;
CREATE TABLE ods1.rst_turnover_feat (
	shop_code varchar(20) NULL, -- 门店编码
	sale_date date NULL, -- 日期
	date_case varchar NULL, -- case
	turnover numeric NULL, -- 营业额
	pre numeric NULL, -- 降雨量特征
	temp numeric NULL, -- 温度特征
	recent_3 numeric NULL, -- 近三天平均营业额
	recent_7 numeric NULL -- 近七天平均营业额
);
truncate table ods1.rst_turnover_feat;
insert into ods1.rst_turnover_feat
(select b.map_columm_code,
sale_date,
date_case,
turnover,
pre,
temp,
recent_3,
recent_7
from rst.rst_turnover_feat a 
left join test.map_store_code_2 b on a.shop_code=b.store_code
where a.shop_code in ('010944','081246','021339') and sale_date>='2018-09-03' and sale_date<='2018-12-14');

drop table if exists ods1.rst_digital_warehouse;
CREATE TABLE ods1.rst_digital_warehouse (
shop_code varchar(20) NULL, 
product_code varchar(20) NULL, 
check_date date NULL,
shelf_life varchar NULL, 
end_qty numeric NULL, 
digital_warehouse varchar NULL 
);
truncate table ods1.rst_digital_warehouse;
insert into ods1.rst_digital_warehouse
(select b.map_columm_code,
c.map_columm_code,
check_date,
case when shelf_life is not  null then substring(cast(shelf_life as varchar) from  '(\d*) days')
     else null
end shelf_life,
end_qty,
digital_warehouse
from rst.rst_digital_warehouse a 
left join test.map_store_code b on a.shop_code=b.store_code
left join test.map_product_code c on a.product_code=c.product_code
where b.map_columm_code is not null and c.map_columm_code is not null);



```