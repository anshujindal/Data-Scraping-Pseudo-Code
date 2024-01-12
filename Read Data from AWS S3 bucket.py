pip install boto3
pip install pandas
#pip install s3fs
#pip install awswrangler

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import trim
from pyspark.sql import functions as F
from datetime import datetime
import time

import boto3
import csv
import pandas as pd
from io import StringIO  

spark = SparkSession.builder.master("yarn").appName('SparkByExamples.com').getOrCreate()

#Create an S3 client
s3_client = boto3.client('s3', aws_access_key_id='ACCESS-KEY-ID', aws_secret_access_key='SECRET-ACCESS-KEY')

#Read an object from the bucket
response = s3_client.get_object(Bucket='BUCKET-NAME', Key='PATH/TO/THE/FILE')

#Read the object’s content as text
object_content = response['Body'].read().decode('utf-8')

a=object_content.split("\n")

space=''
words = spark.sparkContext.parallelize(a)
words1 = words.filter(lambda line: line != space)
header='date,week,month,year,location,rate_month,rate'
fields = [StructField(field_name, StringType(), True)
          for field_name in header.split(',')]
schema = StructType(fields)
temp_var = words1.map(lambda k: k.split(","))
df = spark.createDataFrame(temp_var, schema)

df.show()

df.createOrReplaceTempView("temp_df")

df = spark.sql ("""select distinct `date` from temp_df order by `date` desc""")
df.show()

df.write.mode('append').insertInto("DB-NAME.TABLE-NAME")

#-----------------------------------------------------------------

s3_client = boto3.client('s3', aws_access_key_id='ACCESS-KEY-ID', aws_secret_access_key='SECRET-ACCESS-KEY')

#Read an object from the bucket
response = s3_client.get_object(Bucket='BUCKET-NAME', Key='PATH/TO/THE/FILE')

#Read the object’s content as text
object_content = response['Body'].read().decode('utf-8')

a=object_content.split("\n")

space=''
words = spark.sparkContext.parallelize(a)
words1 = words.filter(lambda line: line != space)
header='__dimension_alias__,__measure_alias__,__grouping_alias__'
fields = [StructField(field_name, StringType(), True)
          for field_name in header.split(',')]
schema = StructType(fields)
temp_var = words1.map(lambda k: k.split(","))
df = spark.createDataFrame(temp_var, schema)

df.show()

df.write.mode('append').insertInto("DB-NAME.TABLE-NAME")


#-----------------------------------------------------------------

s3_client = boto3.client('s3', aws_access_key_id='ACCESS-KEY-ID', aws_secret_access_key='SECRET-ACCESS-KEY')

#Read an object from the bucket
response = s3_client.get_object(Bucket='BUCKET-NAME', Key='PATH/TO/THE/FILE')
#Read the object’s content as text
object_content = response['Body'].read().decode('utf-8')

a=object_content.split("\n")

space=''
words = spark.sparkContext.parallelize(a)
words1 = words.filter(lambda line: line != space)
header='date,week,month,year,location,rate_month,:id,rate'
fields = [StructField(field_name, StringType(), True)
          for field_name in header.split(',')]
schema = StructType(fields)
temp_var = words1.map(lambda k: k.split(","))
df = spark.createDataFrame(temp_var, schema)

df.show()

df.write.mode('append').insertInto("DB-NAME.TABLE-NAME")







