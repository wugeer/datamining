# map_product_code
```
drop table if exists tmp1;
	create temp table tmp1 as 
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
	
	drop table if exists tmp2;
	create temp table tmp2 as 
	select product_code
	from tmp1
	where not exists(select product_code from test.map_product_code_2);
    
	insert into test.map_product_code_2 (product_code) select product_code from tmp2;
	update test.map_product_code_2 set map_columm_code='pr_code_'||to_char(id, 'fm0009') where map_columm_code is null;

	drop table if exists tmp1;
	drop table if exists tmp2;
```