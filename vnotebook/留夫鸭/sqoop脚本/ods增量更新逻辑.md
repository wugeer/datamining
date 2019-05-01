# ods增量更新逻辑
##  大体上是按照有业务时间(事实表)的增量更新,没有的是全量更新(维表)
  
+ 事实表:msn_weather,ods_itm1,ods_ocus,ods_ohem,ods_oitg,ods_oitt,ods_osal,ods_ostr,ods_owor,ods_owsr,ods_owsl,ods_product_bill_other,ods_product_bill_shop,ods_sal1,ods_sal2,ods_sck1,ods_str1,ods_target_product_turnover,ods_wsr1,ods_wsl1

+ 维表:ods_customer_501,ods_customer_801,ods_display,ods_oitm,ods_oshp,ods_ousr,ods_sa_personuprice_501,ods_sa_personuprice_801,ods_shelf_life,
      
      
+ 全量更新:ods_customer_501,ods_customer_801,ods_display,ods_oitt,ods_shelf_life
+ msn_weather 通过date这个字段来控制增量
+ ods_sa_personuprice_501,ods_sa_personuprice_801 通过dstartdate来实现增量更新
+ ods_target_product_turnover 通过sale_date来实现增量
+ ods_ohem,ods_ousr,ods_oshp,ods_ocus,ods_oitm 通过docentry增量
+ ods_product_bill_other,ods_product_bill_shop 通过bill_date增量
+ ods_oitg,ods_itm1,ods_wsr1,ods_wsl1,ods_sck1,ods_sal1,ods_sal2,ods_owsr,ods_owsl,ods_osal 通过etl_time增量更新
+ ods_owor,ods_ostr 通过docdate增量更新
+ ods_str1 通过exitdate增量更新
+ ods_wor1 通过duedate增量更新












