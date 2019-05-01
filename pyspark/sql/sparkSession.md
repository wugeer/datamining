# sparkSession

1.  sparksession是spark编程使用dataset和dataframe的API
2. SparkSession实例可以用来创建dataframe,将dataframe注册成表，对表执行SQL，缓存表和读parquet文件。
> spark = SparkSession.builder \
        .master("local") \
       .appName("Word Count") \
       .config("spark.some.config.option", "some-value") \
        .getOrCreate()
        
  ## builder类
      1. appName(name) 设置程序的名字，在webUI界面显示的名字；如果不设置这个，那么将会生成一个随机的名字；
      2. config(key=None, value=None, conf=None)设置一个配置项，使用这个方法设置的配置项将会自动覆盖SparkConf 和     SparkSession的默认配置。
      3. enableHiveSupport()---允许hive支持，包括连接一致的hive元仓库，支持hive核心和hive用户自定义函数
      4. getOrCreate()-----得到一个已经存在的SparkSession实例或者基于自定义的配置项生成一个新的SparkSession；这个方法先检查是否有一个有效的的全局默认的sparkSession，如果有，返回它；否则创建一个新的sparkSession，并将它作为全局默认的sparkSession；且如果已经有一个默认的sparkSession了，那么设置的配置项将会应用到这个sparkSession。
      5. master(master)----设置要连接的spark 主节点的URL；local代表本地运行，local[4]代表本地以4核来运行；yarn代表使用yarn来调度，spark://master:7077运行在spark独立集群
      ps:如果已经存在的SparkConf，使用conf参数
>>> from pyspark.conf import SparkConf
> SparkSession.builder.config(conf=SparkConf())
> 参数:key------要设置的配置项
> value:--------要设置的配置项的值
> conf ：-------SparkConf实例
    
3. SparkSession.catalog-----用户可以创建，删除，修改，查询底层数据库，表，函数等的接口
4. SparkSession.conf--------spark运行时的配置接口；用户可以得到或设置spark SQL相关的spark和Hadoop设置，当获取配置的值时，默认是基于SparkContext的，如果有的话。
5. SparkSession.createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)---从rdd,List或者pandas.DataFrame创建dataframe。
> 如果schema是列名的列表，每一列的数据类型将从data中推断；
> 如果schema没有设置（或者为none），将从data中推断schema的列名和数据类型，此时data应该是行rdd，命名元祖或者字典；
> 如果schema是pyspark.sql.types.DataType或者 a datatype string，必须匹配真正的数据，否则运行时将会抛出异常。如果提供的schema不是 pyspark.sql.types.StructType类型，它将会被封装成 pyspark.sql.types.StructType作为每一个字段，字段名字将会是value，每一条记录将会被封装成一个元祖，可以在后面被转换为行。
> 如果需要schema，samplingRatio被使用去决定schema接口的行比，将会使用第一行如果samplingRatio 是none。
> 参数：data ----任何种类的SQL数据表示（行，元祖，整型，布尔等），列表，pandas.DataFrame的rdd
> schema---- pyspark.sql.types.DataType 或者 一个表明数据类型的字符串 or 列名的列表，默认是none，数据类型的字符串等价于pyspark.sql.types.DataType.simpleString，高层次的struct类型，可以忽略struct<>；自动类型使用typeName()作为他们的类型；例如，对于pyspark.sql.types.ByteType，使用byte而不是tinyint,也可以使用int代替IntegerType
> samplingRatio ---用于推断的行的采样率
> verifySchema ----根据schema验证每行的数据类型。
> 返回dataframe
6. SparkSession.newSession()----返回一个新的SparkSession作为新的session，它有独立的 SQLConf，注册临时视图和udfs,但是共享了SparkContext 和缓存表
7. SparkSession.range(start, end=None, step=1, numPartitions=None)---创建一个名字为ID，类型为pyspark.sql.types.LongType的DataFrame，包含元素从start开始，到end结束，步长为step，不包括end
> 参数：start----开始值
> end-----结束值（不含这个值）
> step----增长步长（默认是1）
> numPartitions ---DataFrame的分区数
8. SparkSession.read---返回一个可以读入数据作为DataFrame的 DataFrameReader
> 返回值：DataFrameReader
9. SparkSession.readStream----返回一个可以读入数据流作为流的DataFrame的 DataStreamReader
> 返回值：DataStreamReader
10. SparkSession.sparkContext---返回默认底层的SparkContext
11. SparkSession.sql(sqlQuery)---返回一个给定查询的结果的dataframe
> 返回值：DataFrame
12. SparkSession.stop()---停止默认的底层的SparkContext
13. SparkSession.streams--返回一个StreamingQueryManager，可以管理所有的StreamingQuery，在此上下文上活动的流式查询
14. SparkSession.table(tableName)---将具体的表转为dataframe
15. SparkSession.udf----返回一个可以用来注册UDF的UDFRegistration
16. SparkSession.version---返回当前程序正在运行的spark版本