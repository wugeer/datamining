# test
```
# coding: utf-8
# 在执行前注意不要在服务器上面直接改,在另外一个schame上面改
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext, HiveContext,catalog,SparkSession
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType


spark = SparkSession.builder.master("yarn").enableHiveSupport().appName("dim_store_product").getOrCreate()
spark.sql("drop table rbu_sxcp_edw_dev.dim_store_product_1")
print("删除临时表成功")
print("process successfully!")
print("=====================")
```




```flowchart
st=>start: Start:>http://www.google.com[blank]
e=>end:>http://www.google.com
op1=>operation: My Operation
sub1=>subroutine: My Subroutine
cond=>condition: Yes
or No?:>http://www.google.com
io=>inputoutput: catch something...
 
st->op1->cond
cond(yes)->io->e
cond(no)->sub1(right)->op1
```