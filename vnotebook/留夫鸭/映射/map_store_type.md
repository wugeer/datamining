# map_store_type
```
drop table if exists tmp1;
    create temp table tmp1 as 
    select shptype store_type
    from ods.ods_oshp
    union
    select cusgroup store_type
    from ods.ods_ocus;
   
   drop table if exists tmp2;
   create temp table tmp2 as 
   select store_type
   from tmp1 
   where not exists (select store_type from test.map_store_type);
  
   insert into test.map_store_type(store_type) select store_type from tmp2;
   update test.map_store_type set store_type='st_type_'||to_char(id, 'fm0009') where store_type is null;
  
  drop table if exists tmp1;
  drop table if exists tmp2;
```