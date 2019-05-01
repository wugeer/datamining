from pyspark.sql import SQLContext,HiveContext,UDFRegistration,SparkSession
from pyspark.sql.functions import udf
from datetime import datetime, date, timedelta
import re
import time

spark = SparkSession.builder.master("yarn").appName("digital_warehouse").enableHiveSupport().getOrCreate()
item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
print("starting.."+'.'*30)

tmp1_sql = """

"""
