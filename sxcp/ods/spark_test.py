from pyspark.sql import  SparkSession
import sys
import numpy as np
import time
from datetime import datetime, date, timedelta
spark = SparkSession.builder.master("local").appName("test_spark").enableHiveSupport().getOrCreate()
print("starting..........")
print(sys.argv[1])
print("end"+"..."*10)

