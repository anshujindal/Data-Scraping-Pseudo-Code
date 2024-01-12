#pip install pandas

import pandas as pd
from pandas import melt
import numpy as np
import requests
import json
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col,lit,split
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import trim
from pyspark.sql import functions as F
from datetime import datetime
import time



spark = SparkSession.builder.master("yarn").appName('SparkByExamples.com').getOrCreate()



# Henry Read data
your_url = 'URL'

with urllib.request.urlopen(your_url) as url:
    data = json.loads(url.read().decode())
#print(data)
    
df = pd.json_normalize(data['value']['timeSeries'], record_path = ['values','value'])
#print(df)

df['sitename'] = pd.Series(["ILLINOIS RIVER AT HENRY, IL" for x in range(len(df.index))])
df1 = df.rename(columns={'value': 'gage_height_feet'})

print(df1)

#-----------------------------

# Memphis Read data
your_url = 'URL'

with urllib.request.urlopen(your_url) as url:
    data = json.loads(url.read().decode())
#print(data)
    
df = pd.json_normalize(data['value']['timeSeries'], record_path = ['values','value'])
print(df)

df['sitename'] = pd.Series(["MISSISSIPPI RIVER AT MEMPHIS, TN" for x in range(len(df.index))])
df2 = df.rename(columns={'value': 'gage_height_feet'})

print(df2)

#-----------------------------

# St. Louis Read data
your_url = 'URL'

with urllib.request.urlopen(your_url) as url:
    data = json.loads(url.read().decode())
#print(data)
    
df = pd.json_normalize(data['value']['timeSeries'], record_path = ['values','value'])
print(df)

df['sitename'] = pd.Series(["Mississippi River at St. Louis, MO" for x in range(len(df.index))])
df3 = df.rename(columns={'value': 'gage_height_feet'})

print(df3)

#-----------------------------

df_merged = df1.append(df2, ignore_index=True)
df_final = df_merged.append(df3,ignore_index=True)

#-----------------------------

sparkDF=spark.createDataFrame(df_final)

sparkDF.write.mode('append').insertInto("DB-NAME.TABLE-NAME")





