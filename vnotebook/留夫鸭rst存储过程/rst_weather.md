# rst_weather
```
CREATE OR REPLACE FUNCTION rst.p_rst_weather(date)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
	COST 100.0
AS $function$
	BEGIN

drop table if exists tmp1;
create temporary table tmp1 as 
select 
	distinct name city,
	date weather_date,
	high,
	low,
	split_part(split_part(temperatures,',',1),':',2)::numeric temp_0,
	split_part(split_part(temperatures,',',2),':',2)::numeric temp_1,
	split_part(split_part(temperatures,',',3),':',2)::numeric temp_2,
	split_part(split_part(temperatures,',',4),':',2)::numeric temp_3,
	split_part(split_part(temperatures,',',5),':',2)::numeric temp_4,
	split_part(split_part(temperatures,',',6),':',2)::numeric temp_5,
	split_part(split_part(temperatures,',',7),':',2)::numeric temp_6,
	split_part(split_part(temperatures,',',8),':',2)::numeric temp_7,
	split_part(split_part(temperatures,',',9),':',2)::numeric temp_8,
	split_part(split_part(temperatures,',',10),':',2)::numeric temp_9,
	split_part(split_part(temperatures,',',11),':',2)::numeric temp_10,
	split_part(split_part(temperatures,',',12),':',2)::numeric temp_11,
	split_part(split_part(temperatures,',',13),':',2)::numeric temp_12,
	split_part(split_part(temperatures,',',14),':',2)::numeric temp_13,
	split_part(split_part(temperatures,',',15),':',2)::numeric temp_14,
	split_part(split_part(temperatures,',',16),':',2)::numeric temp_15,
	split_part(split_part(temperatures,',',17),':',2)::numeric temp_16,
	split_part(split_part(temperatures,',',18),':',2)::numeric temp_17,
	split_part(split_part(temperatures,',',19),':',2)::numeric temp_18,
	split_part(split_part(temperatures,',',20),':',2)::numeric temp_19,
	split_part(split_part(temperatures,',',21),':',2)::numeric temp_20,
	split_part(split_part(temperatures,',',22),':',2)::numeric temp_21,
	split_part(split_part(temperatures,',',23),':',2)::numeric temp_22,
	split_part(split_part(split_part(temperatures,',',24),':',2),'}',1)::numeric temp_23,
	split_part(precipitations,'""',4)::numeric pre_0,
	split_part(precipitations,'""',8)::numeric pre_1,
	split_part(precipitations,'""',12)::numeric pre_2,
	split_part(precipitations,'""',16)::numeric pre_3,
	split_part(precipitations,'""',20)::numeric pre_4,
	split_part(precipitations,'""',24)::numeric pre_5,
	split_part(precipitations,'""',28)::numeric pre_6,
	split_part(precipitations,'""',32)::numeric pre_7,
	split_part(precipitations,'""',36)::numeric pre_8,
	split_part(precipitations,'""',40)::numeric pre_9,
	split_part(precipitations,'""',44)::numeric pre_10,
	split_part(precipitations,'""',48)::numeric pre_11,
	split_part(precipitations,'""',52)::numeric pre_12,
	split_part(precipitations,'""',56)::numeric pre_13,
	split_part(precipitations,'""',60)::numeric pre_14,
	split_part(precipitations,'""',64)::numeric pre_15,
	split_part(precipitations,'""',68)::numeric pre_16,
	split_part(precipitations,'""',72)::numeric pre_17,
	split_part(precipitations,'""',76)::numeric pre_18,
	split_part(precipitations,'""',80)::numeric pre_19,
	split_part(precipitations,'""',84)::numeric pre_20,
	split_part(precipitations,'""',88)::numeric pre_21,
	split_part(precipitations,'""',92)::numeric pre_22,
	split_part(precipitations,'""',96)::numeric pre_23
from ods.msn_weather 
where temperatures like '"{_%}"'
	and precipitations like '"{_%}"'
	and "date">=$1
	and "date"<=($1+'8 day'::interval)::date
;

delete from rst.rst_weather 
where weather_date>=$1
	and weather_date<=($1+'7 day'::interval)::date
;
insert into rst.rst_weather 
(
	city,
	weather_date,
	high,
	low,
	temp_3,
	temp_4,
	temp_5,
	temp_6,
	temp_7,
	temp_8,
	temp_9,
	temp_10,
	temp_11,
	temp_12,
	temp_13,
	temp_14,
	temp_15,
	temp_16,
	temp_17,
	temp_18,
	temp_19,
	temp_20,
	temp_21,
	temp_22,
	temp_23,
	temp_0,
	temp_1,
	temp_2,
	pre_3,
	pre_4,
	pre_5,
	pre_6,
	pre_7,
	pre_8,
	pre_9,
	pre_10,
	pre_11,
	pre_12,
	pre_13,
	pre_14,
	pre_15,
	pre_16,
	pre_17,
	pre_18,
	pre_19,
	pre_20,
	pre_21,
	pre_22,
	pre_23,
	pre_0,
	pre_1,
	pre_2
)
select 
	t1.city||'市',
	t1.weather_date,
	t1.high,
	t1.low,
	t1.temp_3,
	t1.temp_4,
	t1.temp_5,
	t1.temp_6,
	t1.temp_7,
	t1.temp_8,
	t1.temp_9,
	t1.temp_10,
	t1.temp_11,
	t1.temp_12,
	t1.temp_13,
	t1.temp_14,
	t1.temp_15,
	t1.temp_16,
	t1.temp_17,
	t1.temp_18,
	t1.temp_19,
	t1.temp_20,
	t1.temp_21,
	t1.temp_22,
	t1.temp_23,
	t2.temp_0,
	t2.temp_1,
	t2.temp_2,
	t1.pre_3,
	t1.pre_4,
	t1.pre_5,
	t1.pre_6,
	t1.pre_7,
	t1.pre_8,
	t1.pre_9,
	t1.pre_10,
	t1.pre_11,
	t1.pre_12,
	t1.pre_13,
	t1.pre_14,
	t1.pre_15,
	t1.pre_16,
	t1.pre_17,
	t1.pre_18,
	t1.pre_19,
	t1.pre_20,
	t1.pre_21,
	t1.pre_22,
	t1.pre_23,
	t2.pre_0,
	t2.pre_1,
	t2.pre_2
from tmp1 t1
left join tmp1 t2 
	on t1.city=t2.city 
	and t1.weather_date=(t2.weather_date-'1 day'::interval)::date
where t1.weather_date>=$1
	and t1.weather_date<=($1+'7 day'::interval)::date
;

drop table tmp1;

	RETURN 1;
END

$function$
;

```