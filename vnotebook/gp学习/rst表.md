# rst表
```
seven_days_turnover_ranking
store_code string,
product_code,
sale_date,
sale_total_qty,  --不算断货   当天销量
sale_price,
turnover,
rank
etl_time


check_daily_stock

store_code
product_code
check_date
calculate_end_qty,
actual_end_qty
check_status int  0 未盘点,1 正常,2 不正常
etl_time
```