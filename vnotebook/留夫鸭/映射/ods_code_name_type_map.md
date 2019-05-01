# ods_ohem表code,name映射
## 更新一下store_code,考虑增量
store_code增量逻辑:
先把没有建立映射的store_code插入映射表中,利用id是自增的,此时map_columm_code是null,后面再根据map_columm_code是为null,利用'st_code_'||to_char(id, 'fm0009')拼接得到map_columm_code,然后在全量更新ods相关的表


```
drop table if exists test.map_store_code_2;
create table test.map_store_code_2
(id serial primary key,
store_code text NULL,
map_columm_code text NULL);

drop table if exists tmp1;
create temp table tmp1 as 
select 
substring(map_columm_code from 9)::int id,
store_code,
map_columm_code
from test.map_store_code;
truncate table test.map_store_code_2;
insert into test.map_store_code_2
(id,store_code,map_columm_code)
select id,store_code,map_columm_code
from tmp1 
order by id asc;
```
```
drop table if exists test.map_ohem_code_name;
CREATE TABLE test.map_ohem_code_name (
	user_code text NULL,
	map_columm_code text null,
	user_name text NULL,
	map_columm_name text null
);
drop table if exists tmp1;
create temp table tmp1(id serial ,usercode text, username text);
insert into tmp1 (usercode,username)
select empcode,empname 
from ods.ods_ohem
group by empcode,empname;


truncate table test.map_ohem_code_name;
INSERT INTO test.map_ohem_code_name
select 
	 usercode user_code, 'us_code_'||to_char(id, 'fm0009') as map_columm_code,
	 username user_name, 'us_name_'||to_char(id, 'fm0009') as map_columm_name
from tmp1;

```


# ods_ousr表code,name,maker映射

```
drop table if exists test.map_ousr_code_name;
CREATE TABLE test.map_ousr_code_name (
	user_code text NULL,
	map_columm_code text null,
	user_name text NULL,
	map_columm_name text null,
	maker text null,
	map_column_maker text null
);

drop table if exists tmp1;
create temp table tmp1(id serial ,usercode text, username text,maker text);
insert into tmp1 (usercode,username,maker)
select usercode,username,maker
from ods.ods_ousr
group by usercode,username,maker;

truncate table test.map_ousr_code_name;
INSERT INTO test.map_ousr_code_name
(user_code, map_columm_code,user_name,map_columm_name,maker,map_column_maker)
select 
	usercode user_code, 'us_code_'||to_char(id, 'fm0009') as map_columm_code,
	username user_name, 'us_name_'||to_char(id, 'fm0009') as map_columm_name,
	maker, 'maker_'||to_char(id, 'fm0009') as map_columm_maker
from tmp1;

```

# 获取所有的store_code以及映射
```
drop table if exists tmp1;
create temp table tmp1 as 
select shpcode shop_Code
from ods.ods_oshp
union
SELECT ccuscode shop_Code
FROM ods.ods_customer_501
union
SELECT ccuscode shop_Code
FROM ods.ods_customer_801
union
SELECT shop_code
FROM ods.ods_product_bill_shop
union
SELECT shop_code
FROM ods.ods_product_bill_other
union
SELECT shop_code
FROM ods.ods_shelf_life
union
SELECT shop_code
FROM ods.ods_target_product_turnover
union
SELECT shop_code
FROM ods.ods_display
union
select cuscode shop_code
from ods.ods_ocus;

drop table if exists test.map_store_code ;
create table test.map_store_code
(store_code text NULL,map_columm_code text null);
insert into  test.map_store_code
select 
	shop_code store_code, 'st_code_'||to_char(row_number() over(order by shop_code), 'fm0009') as map_columm_code
from tmp1;
```


# ods_oshp表code,name,type映射

```
drop table if exists test.map_oshp_code_name;
CREATE TABLE test.map_oshp_code_name (
	store_code text NULL,
	map_columm_code text null,
	store_name text NULL,
	map_columm_name text null,
	store_type text NULL,
	map_columm_type text null
);
truncate table test.map_oshp_code_name;
INSERT INTO test.map_oshp_code_name
select 
	shpcode store_code, b.map_columm_code as map_columm_code,
	shpname store_name, 'st_name_'||substring(b.map_columm_code,9,4) as map_columm_name,
	shptype store_type, 'st_type_'||substring(b.map_columm_code,9,4) as map_columm_type
from ods.ods_oshp a
left join test.map_store_code b on a.shpcode=b.store_code

```
# ods_ocus表code,name映射

```
drop table if exists test.map_ocus_code_name_type;
CREATE TABLE test.map_ocus_code_name_type (
	store_code text NULL,
	map_columm_code text null,
	store_name text NULL,
	map_columm_name text null,
	store_type text NULL,
	map_columm_type text null
);

truncate table test.map_ocus_code_name_type;
INSERT INTO test.map_ocus_code_name_type
select 
	cuscode store_code, b.map_columm_code as map_columm_code,
	cusname store_name, 'st_name_'||substring(b.map_columm_code,9,4) as map_columm_name,
	cusgroup store_type, 'st_type_'||substring(b.map_columm_code,9,4) as map_columm_type
from ods.ods_ocus a
left join test.map_store_code b on a.cuscode=b.store_code
group by cuscode,cusname,cusgroup,b.map_columm_code;
```

# ods_customer_501表code,name映射

```
drop table if exists test.map_customer_501_code_name;
CREATE TABLE test.map_customer_501_code_name (
	store_code text NULL,
	map_columm_code text null,
	store_name text NULL,
	map_columm_name text null
);

truncate table test.map_customer_501_code_name;
INSERT INTO test.map_customer_501_code_name
select 
	ccuscode store_code, b.map_columm_code as map_columm_code,
	ccusname store_name, 'st_name_'||substring(b.map_columm_code,9,4) as map_columm_name
from ods.ods_customer_501 a
left join test.map_store_code b on a.ccuscode=b.store_code
group by ccuscode,ccusname,b.map_columm_code;


```

# ods_customer_801表code,name映射

```
drop table if exists test.map_customer_801_code_name;
CREATE TABLE test.map_customer_801_code_name (
	store_code text NULL,
	map_columm_code text null,
	store_name text NULL,
	map_columm_name text null
);

truncate table test.map_customer_801_code_name;
INSERT INTO test.map_customer_801_code_name
select 
	ccuscode store_code, b.map_columm_code as map_columm_code,
	ccusname store_name, 'st_name_'||substring(b.map_columm_code,9,4) as map_columm_name
from ods.ods_customer_801 a
left join test.map_store_code b on a.ccuscode=b.store_code
group by ccuscode,ccusname,b.map_columm_code;
```

# ods_product_bill_shop表code,name映射
```
drop table if exists test.map_product_bill_shop_store_code_name;
CREATE TABLE test.map_product_bill_shop_store_code_name (
	store_code text NULL,
	map_columm_code text null,
	store_name text NULL,
	map_columm_name text null
);

truncate table test.map_product_bill_shop_store_code_name;
INSERT INTO test.map_product_bill_shop_store_code_name
select 
	shop_code store_code, b.map_columm_code as map_columm_code,
	shop_name store_name, 'st_name_'||substring(b.map_columm_code,9,4) as map_columm_name
from ods.ods_product_bill_shop a
left join test.map_store_code b on a.shop_code=b.store_code
group by shop_code,shop_name,b.map_columm_code;
```

# ods_product_bill_other表code,name映射

```
drop table if exists test.map_product_bill_other_store_code_name;
CREATE TABLE test.map_product_bill_other_store_code_name (
	store_code text NULL,
	map_columm_code text null,
	store_name text NULL,
	map_columm_name text null
);

truncate table test.map_product_bill_other_store_code_name;
INSERT INTO test.map_product_bill_other_store_code_name
select 
	shop_code store_code, b.map_columm_code as map_columm_code,
	shop_name store_name, 'st_name_'||substring(b.map_columm_code,9,4) as map_columm_name
from ods.ods_product_bill_other a
left join test.map_store_code b on a.shop_code=b.store_code
group by shop_code,shop_name,b.map_columm_code;
```




# product_code获取及映射
```
drop table if exists tmp1;
create temp table tmp1 (id serial, product_code varchar);
insert into tmp1 (product_code)
select itemcode product_code
from ods.ods_oitm
union
select product_code
from ods.ods_display 
union
select stdcode product_code
from ods.ods_oitt
union
select product_code
from ods.ods_product_bill_other
union
select product_code
from ods.ods_product_bill_shop
union
select product_code
from ods.ods_shelf_life
union
select product_code
from ods.ods_target_product_turnover
union
select cinvcode product_code
from ods.ods_sa_personuprice_501
union
select cinvcode product_code
from ods.ods_sa_personuprice_801;
drop table if exists test.map_product_code ;
create table test.map_product_code
(product_code text NULL,map_columm_code text null);
insert into  test.map_product_code
select 
	product_code, 'pr_code_'||to_char(id, 'fm0009') as map_columm_code
from tmp1;
```
### product_code更新逻辑
```
drop table if exists test.map_product_code_2;
create table test.map_product_code_2
(id serial primary key,
product_code text NULL,
map_columm_code text NULL);

drop table if exists tmp1;
create temp table tmp1 as 
select 
substring(map_columm_code from 9)::int id,
product_code,
map_columm_code
from test.map_product_code;
truncate table test.map_product_code_2;
insert into test.map_product_code_2
(id,product_code,map_columm_code)
select id,product_code,map_columm_code
from tmp1 
order by id asc;
```

# oitm表映射code,name,type

```
drop table if exists test.map_oitm_code_name_type;
CREATE TABLE test.map_oitm_code_name_type (
	product_code text NULL,
	map_columm_code text null,
	product_name text NULL,
	map_columm_name text null,
	product_type text NULL,
	map_columm_type text null
);

truncate table test.map_oitm_code_name_type;
INSERT INTO test.map_oitm_code_name_type
(product_code, map_columm_code,product_name,map_columm_name,product_type,map_columm_type)
select 
	itemcode product_code, b.map_columm_code,
	itemname product_name, 'pr_name_'||substring(b.map_columm_code,9,4) as map_columm_name,
	a8 product_type, 'pr_type_'||substring(b.map_columm_code,9,4) as map_columm_type
from ods.ods_oitm a
left join test.map_product_code b on a.itemcode=b.product_code
group by itemcode,itemname,a8, b.map_columm_code;
```

# oitt表映射code,name

```
drop table if exists test.map_oitt_code_name;
CREATE TABLE test.map_oitt_code_name (
	product_code text NULL,
	map_columm_code text null,
	product_name text NULL,
	map_columm_name text null
);

truncate table test.map_oitt_code_name;
INSERT INTO test.map_oitt_code_name
select 
	stdcode product_code, b.map_columm_code,
	stdname product_name, 'pr_name_'||substring(b.map_columm_code,9,4) as map_columm_name
from ods.ods_oitt a
left join test.map_product_code b on a.stdcode=b.product_code
group by stdcode,stdname,b.map_columm_code;
```

# ods_product_bill_other_product表映射code,name,type

```
drop table if exists test.map_product_bill_other_product_code_name_type;
CREATE TABLE test.map_product_bill_other_product_code_name_type (
	product_code text NULL,
	map_columm_code text null,
	product_name text NULL,
	map_columm_name text null,
	product_type text NULL,
	map_columm_type text null
);

truncate table test.map_product_bill_other_product_code_name_type;
INSERT INTO test.map_product_bill_other_product_code_name_type
select 
	a.product_code, b.map_columm_code,
	product_name, 'pr_name_'||substring(b.map_columm_code,9,4) as map_columm_name,
	product_type, 'pr_type_'||substring(b.map_columm_code,9,4) as map_columm_type
from ods.ods_product_bill_other a
left join test.map_product_code b on a.product_code=b.product_code
group by a.product_code,product_name,product_type, b.map_columm_code;
```

# ods_product_bill_shop_product表映射code,name,type

```
drop table if exists test.map_product_bill_shop_product_code_name_type;
CREATE TABLE test.map_product_bill_shop_product_code_name_type (
	product_code text NULL,
	map_columm_code text null,
	product_name text NULL,
	map_columm_name text null,
	product_type text NULL,
	map_columm_type text null
);

truncate table test.map_product_bill_shop_product_code_name_type;
INSERT INTO test.map_product_bill_shop_product_code_name_type
select 
	a.product_code, b.map_columm_code,
	product_name, 'pr_name_'||substring(b.map_columm_code,9,4) as map_columm_name,
	product_type, 'pr_type_'||substring(b.map_columm_code,9,4) as map_columm_type
from ods.ods_product_bill_shop a
left join test.map_product_code b on a.product_code=b.product_code
group by a.product_code,product_name,product_type, b.map_columm_code;
```




