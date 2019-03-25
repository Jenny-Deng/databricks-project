# Databricks notebook source
ACCESS_KEY = "xxxxxxx"
SECRET_KEY = "xxxxxxxxxxxxxxxxxxxxx"

# COMMAND ----------

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName('My Spark App') \
        .enableHiveSupport() \
        .getOrCreate()
print('Session created')

sc = spark.sparkContext

# COMMAND ----------

df_loc = spark.read.csv('s3a://data-hub-homework-data/location.csv', header='true')
df_prod = spark.read.csv('s3a://data-hub-homework-data/product.csv', header='true')

# COMMAND ----------

# from pyspark.sql.types import StructType, StructField
# from pyspark.sql.types import DoubleType, IntegerType, StringType, LongType
# different files have difference orders 
# trans_schema1 = StructType([\
#         StructField("store_location_key", StringType(), True),\
#         StructField("product_key", StringType(), True), \
#         StructField("collector_key", LongType(), True),\
#         StructField("trans_dt", StringType(), True),\
#         StructField("sales", DoubleType(), True),\
#         StructField("units", IntegerType(), True),\
#         StructField("trans_key", StringType(), True)\
#     ])

# trans_schema2 = StructType([\
#         StructField("store_location_key", StringType(), True),\
#         StructField("product_key", StringType(), True), \
#         StructField("collector_key", LongType(), True),\
#         StructField("trans_dt", StringType(), True),\
#         StructField("sales", DoubleType(), True),\
#         StructField("units", IntegerType(), True),\
#         StructField("trans_id", StringType(), True)\
#     ])

# COMMAND ----------

df_trans1 = spark.read.csv('s3a://data-hub-homework-data/trans_fact_1.csv', header='true')
df_trans2 = spark.read.csv('s3a://data-hub-homework-data/trans_fact_2.csv', header='true')
df_trans3 = spark.read.csv('s3a://data-hub-homework-data/trans_fact_3.csv', header='true')
df_trans4 = spark.read.csv('s3a://data-hub-homework-data/trans_fact_4.csv', header='true')
df_trans5 = spark.read.csv('s3a://data-hub-homework-data/trans_fact_5.csv', header='true')
df_trans6 = spark.read.csv('s3a://data-hub-homework-data/trans_fact_6.csv', header='true')
df_trans7 = spark.read.csv('s3a://data-hub-homework-data/trans_fact_7.csv', header='true')
df_trans8 = spark.read.csv('s3a://data-hub-homework-data/trans_fact_8.csv', header='true')
df_trans9 = spark.read.csv('s3a://data-hub-homework-data/trans_fact_9.csv', header='true')
df_trans10 = spark.read.csv('s3a://data-hub-homework-data/trans_fact_10.csv', header='true')

# COMMAND ----------

# 1. schema has different order and columm rename  
# 2. fill na with 0
# 3. drop dups
# 4. create a new column: total sales 

# COMMAND ----------

df_t4 = df_trans4.select('store_location_key','product_key','collector_key','trans_dt','sales','units','trans_key')

df_t5 = df_trans5.select('store_location_key','product_key','collector_key','trans_dt','sales','units','trans_key')

df_t6 = df_trans6.withColumnRenamed('trans_id', 'trans_key') \
.select('store_location_key','product_key','collector_key','trans_dt','sales','units','trans_key')

df_t7 = df_trans7.withColumnRenamed('trans_id', 'trans_key') \
.select('store_location_key','product_key','collector_key','trans_dt','sales','units','trans_key')

df_t8 = df_trans8.withColumnRenamed('trans_id', 'trans_key') \
.select('store_location_key','product_key','collector_key','trans_dt','sales','units','trans_key')

df_t9 = df_trans9.withColumnRenamed('trans_id', 'trans_key') \
.select('store_location_key','product_key','collector_key','trans_dt','sales','units','trans_key')

df_t10 = df_trans10.withColumnRenamed('trans_id', 'trans_key') \
.select('store_location_key','product_key','collector_key','trans_dt','sales','units','trans_key')

# COMMAND ----------

from functools import reduce  
from pyspark.sql import DataFrame

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs) 

df_sum = unionAll(*[df_trans1, df_trans2, df_trans3, df_t4, df_t5, df_t6, df_t7, df_t8, df_t9, df_t10])

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col

df_sum.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_sum.columns]).show()

# COMMAND ----------

df_sum.count()

# COMMAND ----------

df_fill_na = df_sum.fillna({'sales': '0', 'units': '0'})

# COMMAND ----------

df_fill_na.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fill_na.columns]).show()

# COMMAND ----------

df_drop_dup = df_fill_na.drop_duplicates() 
df_drop_dup.count()

# COMMAND ----------

df_trans = df_drop_dup.withColumn("units", col("units").cast("int")).withColumn("sales", col("sales").cast("double")) \
.withColumn("total_sales", col("units") * col("sales"))

# COMMAND ----------

# which provinces and stores are performing well 

# COMMAND ----------

trans_loc = df_trans.join(df_loc, df_trans.store_location_key==df_loc.store_location_key, 'left') \
.select(df_trans.store_location_key, df_trans.product_key, df_trans.collector_key, df_trans.total_sales, df_loc.province)

# COMMAND ----------

from pyspark.sql.functions import *

# df_trans.agg(countDistinct("store_location_key")).show() -47
# df_loc.agg(countDistinct("store_location_key")).show() -883

# COMMAND ----------

import pyspark.sql.functions as func
from pyspark.sql.functions import desc
from pyspark.sql.functions import rank 

trans_loc.groupBy("store_location_key") \
.agg(round(sum("total_sales"), 2).alias("total_sales_by_store")) \
.orderBy(desc("total_sales_by_store")).show(5)

trans_loc.groupBy("province") \
.agg(round(sum("total_sales"), 2).alias("total_sales_by_province")) \
.orderBy(desc("total_sales_by_province")).show(10)

# COMMAND ----------

# top stores in each province performing compared with the average store of the province.

# trans_loc.groupBy("store_location_key").agg(sum("total_sales").alias("tt")).show()



trans_loc.filter(col("province").isNotNull()) \
 .groupBy("province") \
.agg(round(max("total_sales"), 2).alias("max_sales_by_province"), 
     round(avg("total_sales"), 2).alias("avg_sales_by_province")) \
.withColumn("diff_sales_by_province", round(col("max_sales_by_province") - col("avg_sales_by_province"), 2)) \
.orderBy(desc("diff_sales_by_province")) \
.show()

# COMMAND ----------

# loyalty program are performing compared to non-loyalty customers 

# COMMAND ----------

trans_loc_loyalty = trans_loc.withColumn("collector_key", col("collector_key").cast("double")) \
.withColumn("loyalty_type", when(col("collector_key") > 0, "Loyalty").otherwise("Non-loyalty")) \


trans_loc_loyalty.groupBy("loyalty_type") \
.agg(round(sum("total_sales"), 2).alias("total_sales_by_loyalty")) \
.show()

# COMMAND ----------

# what category of products is contributing to most of ACME’s sales

# COMMAND ----------

# trans_loc = df_trans.join(df_loc, df_trans.store_location_key==df_loc.store_location_key, 'left') \
# .select(df_trans.store_location_key, df_trans.product_key, df_trans.collector_key, df_trans.total_sales, df_loc.province)

# COMMAND ----------

trans_loc_loyalty.agg(countDistinct("product_key")).show()

# COMMAND ----------

df_prod.agg(countDistinct("product_key")).show()

# COMMAND ----------

###############

# COMMAND ----------

trans_loc_prod = trans_loc_loyalty.join(df_prod.select("product_key", "category", "department"), trans_loc_loyalty.product_key == df_prod.product_key, 'left_outer') \
.drop(df_prod.product_key)

trans_loc_prod.groupBy("category") \
.agg(round(sum("total_sales"), 2).alias("total_sales_by_category")) \
.orderBy(desc("total_sales_by_category")) \
.show(6)

# COMMAND ----------

trans_loc_loyalty.filter(col("product_key") == "1111112114").head(3)

# COMMAND ----------

df_prod.filter(col("product_key") == "1111112114").head(3)

# COMMAND ----------

trans_loc_prod.filter(col("category").isNull()).head(2)

# COMMAND ----------

# top 5 stores by province 

# COMMAND ----------

from pyspark.sql.window import Window 

wSpec1 = Window.partitionBy("province").orderBy(desc("total_sales"))

trans_loc_prod.filter(col('province').isNotNull()) \
.groupBy('province','store_location_key').agg(round(sum("total_sales"), 2).alias("total_sales")) \
.withColumn("rank", rank().over(wSpec1)) \
.filter(col('rank') <= 5) \
.show(50)

# COMMAND ----------

# top 10 product categories by department

# COMMAND ----------

wSpec2 = Window.partitionBy("department").orderBy(desc("total_sales"))

trans_loc_prod.filter(col('department').isNotNull()) \
.groupBy('department','category').agg(round(sum("total_sales"), 2).alias("total_sales")) \
.withColumn("rank", rank().over(wSpec2)) \
.filter(col('rank') <= 10) \
.show(100)

# COMMAND ----------

# trans_loc_prod.filter(col('department').isNotNull()) \
# .groupBy('department','category').agg(round(sum("total_sales"), 2).alias("total_sales")) \
# .withColumn("rank", rank().over(wSpec2)) \
# .filter(col('rank') <= 10) \
# .filter(col('rank') == 1) \
# .count()