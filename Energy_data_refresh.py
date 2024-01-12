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

spark = SparkSession.builder.master("yarn").appName('SparkByExamples.com').getOrCreate()

import requests
import json

import datetime
latest_month = datetime.datetime.now().strftime("%Y-%m")
now = datetime.datetime.now()
result = [now.strftime("%B")]
for x in range(0, 5):
  now = now.replace(day=1) - datetime.timedelta(days=1)
  result.append(now.strftime("%Y-%m"))
last6monthlist = result[1:6]
last6monthlist.append(latest_month)

print(sorted(last6monthlist))

for x in last6monthlist:
    dt= x
    print(dt)
    
    url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/NRG_CB_OILM/M.GID_OBS+IPRD+TOS+RCV_RCY+IMP+EXP+STK_CHG+INTMARB+GID_CAL+INTAVI_E+TI_EHG_MAP+TO_RPI_RO+RL+RI_CAL+RI_OBS+RF+FC_TRA_RAIL_DNAVI_E+FC_TRA_ROAD_E+STATDIFF+BKFLOW+PPR+IT+PT+DU+GD_PI+ND_TP.O4100_TOT_4200-4500+O4100_TOT+O4200+O4300+O4400+O4410+O4500+O4600+O4610+O4620+O4630+O4640+O4651+O4652+O4652XR5210B+O4653+O4661+O4661XR5230B+O4669+O4671+O4671XR5220B+O46711+O46712+O4680+O4681+O4682+O4690XO4694+O4694+R5210B+R5220B+R5230+R5230B.THS_T.EU27_2020+BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE+IS+NO+UK+ME+MD+MK+AL+RS+TR+GE/?format=TSV&startPeriod="+dt+"&endPeriod="+dt
    response = requests.get(url)
    r=response.content
    print(r)
    m = r.decode("utf-8") 
    n = m[0:2]
    if n == "<?":
      print("False")
    else:
      print("True")
      r = r.decode()
      a=r.split("\n")
      print(a)
      space=''
      words = spark.sparkContext.parallelize(a)
      words1 = words.filter(lambda line: line != space)
      header='freq,nrg_bal,siec,unit,geo'
      fields = [StructField(field_name, StringType(), True)
                for field_name in header.split(',')]
      schema = StructType(fields)
      temp_var = words1.map(lambda k: k.split(","))
      df = spark.createDataFrame(temp_var, schema)
      df=df.filter("freq!='freq'")
      df1=df.withColumn('geo', regexp_replace('geo', '\t', ','))
      from pyspark.sql.functions import explode,split
      df2=df1.withColumn("geo", split("geo",",")).withColumn("freq", trim(df1.freq))
      #df3=df2.withColumn("Cal", explode(col("name")))
      df3=df2.withColumn("geo1",df2['geo'][0]).withColumn("value",df2['geo'][1]).drop("geo")
      df4=df3.withColumn("value", trim(df3.value)).withColumn("dt", lit(dt))
      df5=df4.withColumn("SOURCE",lit("EUROSTAT")).withColumn("FREQUENCY",lit("Monthly")).withColumn("UOM",lit("Thousand tonnes")).withColumn('Value', lit(split(df4['Value'], ' ').getItem(0))).withColumnRenamed(
        "siec", "COMMODITY_CODE").withColumnRenamed(
        "geo1", "LOCATION_CODE").withColumnRenamed(
        "nrg_bal", "ENERGY_BALANCE_CODE")
      
      
      df5.createOrReplaceTempView("temp_df")
      
      df = spark.sql("""select distinct dt from temp_df""")
      df.show()
      
      df6 = spark.sql("""select 
      SOURCE, 
      COMMODITY_CODE, 
      "" as COMMODITY,
      LOCATION_CODE,
      "" AS LOCATION,
      concat_ws('-', dt, '01') publish_date,
      UOM, 
      CASE 
      WHEN value LIKE '%:%' THEN NULL 
      ELSE value 
      END value, 
      FREQUENCY, 
      ENERGY_BALANCE_CODE,
      "" AS ENERGY_BALANCE 
      FROM temp_df
      """).createOrReplaceTempView("temp_df1")
      
      
      df7 = spark.sql("""
      SELECT 
      SOURCE, 
      t1.COMMODITY_CODE, 
      t2.commodity_desc as COMMODITY, 
      t1.LOCATION_CODE, 
      t3.location_desc as `LOCATION`, 
      cast(publish_date as timestamp),
      UOM,
      value,
      FREQUENCY, 
      t1.ENERGY_BALANCE_CODE, 
      t4.energy_balance_desc AS ENERGY_BALANCE 
      FROM  temp_df1 t1
      LEFT OUTER JOIN DB.TB t2 ON t1.COMMODITY_CODE = t2.commodity_code 
      LEFT OUTER JOIN DB.TB t3 ON t1.LOCATION_CODE = t3.location_code 
      LEFT OUTER JOIN DB.TB t4 ON t1.ENERGY_BALANCE_CODE = t4.energy_balance_code""").createOrReplaceTempView("temp_df2")
      
      dfexcept = spark.sql("""SELECT * FROM temp_df2 aa WHERE NOT EXISTS (SELECT * FROM DB.TB bb WHERE ( aa.SOURCE = bb.SOURCE AND 
      aa.COMMODITY_CODE = bb.COMMODITY_CODE AND
      aa.COMMODITY = bb.COMMODITY AND
      aa.LOCATION_CODE = bb.LOCATION_CODE AND
      aa.`LOCATION` = bb.`LOCATION` AND
      aa.publish_date = bb.publish_date AND
      aa.UOM = bb.UOM AND
      aa.FREQUENCY = bb.FREQUENCY AND
      aa.ENERGY_BALANCE_CODE = bb.ENERGY_BALANCE_CODE AND
      aa.ENERGY_BALANCE = bb.ENERGY_BALANCE AND
      aa.value = bb.value
      ))""").createOrReplaceTempView("except")
      
      
       df = spark.sql("""
      
      SELECT COUNT(*) FROM (SELECT * FROM except
where commodity_code='O4652' and energy_balance_code='IMP' and publish_date ='2023-03-01 00:00:00' 
and `location_code` in ("AT","BG","CY","CZ","Estonia","EL","EU27_2020",
"FI","FR","HR","NL","PL","SE","SK","TR")) F
      
      """)
      df.show()
    
      
      dffinal = dfexcept.withColumn("UPDATED_DATE",lit(F.current_timestamp()))
      
      
      dffinal.write.mode('append').insertInto("DB.TB")