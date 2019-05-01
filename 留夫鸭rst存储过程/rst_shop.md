# rst_shop
```
CREATE OR REPLACE FUNCTION rst.p_rst_shop()
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

truncate table rst.rst_shop;
insert into rst.rst_shop 
(
	shop_code,
	shop_name,
	shop_type,
	shop_class,
	price_list,
	address,
	area,
	province,
	city,
	county_district,
	distribute_route,
	is_valid,
	district_manager_code,
	district_manager_name,
	district_director_code,
	district_director_name,
	order_cycle,
	start_date,
	phone_number,
	shop_opening_time,
	shop_closed_time
) 
select 
	shop_code,
	shop_name,
	shop_type,
	shop_class,
	price_list,
	address,
	area,
	province,
	city,
	county_district,
	distribute_route,
	is_valid,
	district_manager_code,
	district_manager_name,
	district_director_code,
	district_director_name,
	order_cycle,
	start_date,
	phone_number,
	shop_opening_time,
	shop_closed_time
from edw.dim_shop
;
	RETURN 1;
END

$function$
;

```