# crontab调度和turnover_predict和sale_predict更新逻辑
* 先梳理下ods,edw和edw_ai表之间的顺序
* gp ods数据大概在早上5点半就会全部导好,那么先导增量的数据,同时准备脱敏全量更新的表的数据,时间点5:30
* edw表从5:10开始跑,大概30个小时就能跑完了,同时将模型跑的门店分类表先脱敏,再导到hive
* 5:40开始跑edw_ai的表,大概需要50分钟
* 6:30跑rst的七天销量排名,这个表10分钟内就能跑完
* 模型开始跑,时间大概是5到10分钟,开始时间是6:45
* 07:00开始生成rst_product_bill和rst_product_qty表,时间在5分钟内
* 将上面跑的那两张表抽到gp的临时表,十分钟内解决,开始时间是07:08
* 调度gp的存构将临时表的数据插入到正式表中,调度时间是07:20

1 更新的调度方案
05:30 开始准备ods1全量导入的数据,同时开始从ods增量导入数据,
06:00 开始全量导入数据
06:40 开始跑edw.sh
07:10 开始跑edw_ai.sh
08:00 开始跑rst.sh

+ 每天on_sale=>销补=>库龄=>turnover_feat=>sale_feat=>turnover_predict=>sale_predict(用昨天的turnover更新之前的turnover和error),这些工作做好后就可以跑模型了
+ 每周更新销补表和turnover_feat和sale_feat的date_case用模型跑出的门店分类表(通过门店日期关联)


## turnover_predict和sale_predict更新逻辑

>  turnover_predict字段解释:turnover和sale_date一一对应,d1,d2,d3,..,d7是今天,明天,后天,...,模型预测的turnover,
> sale_predict字段解释:qty和sale_date一一对应,d1,..,d7分别是今天,明天,..模型预测销量,error1,error2,...分别是模型预测和实际的差
> 更新turnover_predict:插入有了昨天的turnover后,通过门店code和日期(此时有日期的加减操作)关联,将之前数据中涉及昨天的预测的turnover的记录和实际的turnover的更新,以及相应的error也要更新时间跨度是一周,昨天的error是
> 更新sale_predict:通过销补后的获得昨天门店商品的销量后就可以更新qty(昨天的qty,这个在昨天刚跑的时候是不知道实际的qty,所以为null,今天有了昨天的qty就可以更新这个字段)和更新相应的error,通过门店商品日期关联,昨天的error是qty-d1,前天的error是d2-qty,以此类推