# map_store_code
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
	
	drop table if exists tmp2;
    create temp table tmp2 as 
    select shop_code
    from tmp1 a
    where not exists(select store_code from test.map_store_code_2);
   
   insert into test.map_store_code_2(store_code) select shop_code from tmp2;
   update test.map_store_code_2 set map_columm_code='st_code_'||to_char(id, 'fm0009') where map_columm_code is null;
   drop table if exists tmp1;
   drop table if exists tmp2;
```