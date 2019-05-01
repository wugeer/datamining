# ods映射关系
目标: shop_code
来源: oshp.shpcode union customer_801.ccuscode  union customer_501.ccuscode  union store_category.store_code union ods_product_bill_shop.shop_code union ods_product_bill_other.shop_code union ods_shelf_life.shop_code union ods_target_product_turnover.shop_code union ods_display.shop_code

目标:product_code
来源:oitm.itemcode  union ods_product_bill_shop.product_code union ods_shelf_life.shop_code union ods_product_bill_other.product_code union ods_sa_personuprice_501.cinvcode union ods_sa_personuprice_801.cinvcode union ods_display.product_code

目标:product_name
来源:oitm.itemname 

目标:product_type/large_class
来源:oitm.a8


目标:empcode
来源:ohem.empcode union oshp.empdutyid


目标:username
来源:ohem.empname union ousr.username
