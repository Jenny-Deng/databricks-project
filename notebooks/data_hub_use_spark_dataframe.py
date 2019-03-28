# Databricks notebook source
# ACCESS_KEY = "xxxxxxx"
# SECRET_KEY = "xxxxxxxxxxxxxxxxxxxxx"

# COMMAND ----------

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)

# COMMAND ----------

### import location and product files 
df_loc = spark.read.csv('s3a://data-hub-homework-data/location.csv', header='true')
df_prod = spark.read.csv('s3a://data-hub-homework-data/product.csv', header='true')

# COMMAND ----------

### import transaction files 
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

### Data Cleaning - rename and order columns 
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

### combine all transaction files 
from functools import reduce  
from pyspark.sql import DataFrame

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs) 

df_sum = unionAll(*[df_trans1, df_trans2, df_trans3, df_t4, df_t5, df_t6, df_t7, df_t8, df_t9, df_t10])

# COMMAND ----------

### count total number of records 
df_sum.count()

# COMMAND ----------

### check null value columns
from pyspark.sql.functions import isnan, when, count, col

df_sum.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_sum.columns]).show()

# COMMAND ----------

### replace null values with zeros 
df_fill_na = df_sum.fillna({'sales': '0', 'units': '0'})

# COMMAND ----------

### check null values 
df_fill_na.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fill_na.columns]).show()

# COMMAND ----------

### drop duplicates and check number of records
from pyspark.sql.functions import *

df_drop_dup = df_fill_na.drop_duplicates() 
df_drop_dup.count()

# COMMAND ----------

### convert data type 
df_trans = df_drop_dup.withColumn("units", col("units").cast("int")) \
.withColumn("sales", col("sales").cast("double"))

### check number of unique store_location_key values 
df_trans.agg(countDistinct("store_location_key")).show() 
df_loc.agg(countDistinct("store_location_key")).show() 

# COMMAND ----------

### left join transaction table with location table - grab column province and store_num
trans_loc = df_trans.join(df_loc, df_trans.store_location_key==df_loc.store_location_key, 'left') \
.select(df_trans.store_location_key, df_trans.product_key, df_trans.collector_key, df_trans.sales, df_loc.province, df_loc.store_num)

# COMMAND ----------

### 1(1).which provinces and stores are performing well 
trans_loc.groupBy("province") \
.agg(round(sum("sales"), 2).alias("total_sales_by_province")) \
.orderBy(desc("total_sales_by_province")).show(10)

store_sales = trans_loc.groupBy("store_num") \
.agg(round(sum("sales"), 2).alias("total_sales_by_store")) \
.orderBy(desc("total_sales_by_store")).show(5)

# COMMAND ----------

### 1(1).store result to S3

province_sales = trans_loc.groupBy("province") \
.agg(round(sum("sales"), 2).alias("total_sales_by_province")) \
.orderBy(desc("total_sales_by_province")).head(10)

spark.createDataFrame(province_sales).coalesce(1).write.csv("s3a://data-hub-homework-data/analytics_result/province_sales", header = True)

store_sales = trans_loc.groupBy("store_num") \
.agg(round(sum("sales"), 2).alias("total_sales_by_store")) \
.orderBy(desc("total_sales_by_store")).head(5)

spark.createDataFrame(store_sales).coalesce(1).write.csv("s3a://data-hub-homework-data/analytics_result/store_sales", header = True)


# COMMAND ----------

### 1(2).top stores in each province performing compared with the average store of the province.

trans_loc.filter(col("province").isNotNull()) \
 .groupBy("province") \
.agg(round(max("sales"), 2).alias("max_sales_by_province"), 
     round(avg("sales"), 2).alias("avg_sales_by_province")) \
.withColumn("diff_sales_by_province", round(col("max_sales_by_province") - col("avg_sales_by_province"), 2)) \
.orderBy(desc("diff_sales_by_province")) \
.show()

# COMMAND ----------

### 1(2).store result to S3

province_max_avg = trans_loc.filter(col("province").isNotNull()) \
 .groupBy("province") \
.agg(round(max("sales"), 2).alias("max_sales_by_province"), 
     round(avg("sales"), 2).alias("avg_sales_by_province")) \
.withColumn("diff_sales_by_province", round(col("max_sales_by_province") - col("avg_sales_by_province"), 2)) \
.orderBy(desc("diff_sales_by_province")) 

province_max_avg.coalesce(1).write.csv("s3a://data-hub-homework-data/analytics_result/province_max_avg", header = True)

# COMMAND ----------

### generate new column loyalty_type
trans_loc_loyalty = trans_loc.withColumn("collector_key", col("collector_key").cast("double")) \
.withColumn("loyalty_type", when(col("collector_key") > 0, "Loyalty").otherwise("Non-loyalty")) \

### 2(1).customers in the loyalty program performing compared to non-loyalty customers 
loyalty_type = trans_loc_loyalty.groupBy("loyalty_type") \
.agg(round(sum("sales"), 2).alias("total_sales_by_loyalty")) \

loyalty_type.show()

### 2(1).store result to S3

loyalty_type.coalesce(1).write.csv("s3a://data-hub-homework-data/analytics_result/loyalty_type", header = True)

# COMMAND ----------

### left join transaction table with product table - grab column department and category
trans_prod = df_trans.join(df_prod, df_trans.product_key==df_prod.product_key, 'left') \
.select(df_trans.store_location_key, df_trans.product_key, df_trans.collector_key, df_trans.sales, df_prod.department, df_prod.category)

### 2(2). what category of products is contributing to most of ACME’s sales
top_category = trans_prod.filter(col("category").isNotNull()) \
.groupBy("category") \
.agg(round(sum("sales"), 2).alias("total_sales_by_category")) \
.orderBy(desc("total_sales_by_category"))

top_category.show(3)

# COMMAND ----------

### 2(2).store result to S3

spark.createDataFrame(top_category.head(3)).coalesce(1).write.csv("s3a://data-hub-homework-data/analytics_result/top_category", header = True)

# COMMAND ----------

### 3(1). top 5 stores by province 

from pyspark.sql.window import Window 

wSpec1 = Window.partitionBy("province").orderBy(desc("total_sales"))

stores_by_province = trans_loc.filter(col("province").isNotNull()) \
.groupBy("province","store_num").agg(round(sum("sales"), 2).alias("total_sales")) \
.withColumn("rank", rank().over(wSpec1)) \
.filter(col('rank') <= 5) \

stores_by_province.show(50)

### 3(1).store result to S3

stores_by_province.coalesce(1).write.csv("s3a://data-hub-homework-data/analytics_result/stores_by_province", header = True)

# COMMAND ----------

### 3(2). top 10 product categories by department

wSpec2 = Window.partitionBy("department").orderBy(desc("total_sales"))

category_by_deparment = trans_prod.filter(col("department").isNotNull()) \
.groupBy("department","category").agg(round(sum("sales"), 2).alias("total_sales")) \
.withColumn("rank", rank().over(wSpec2)) \
.filter(col('rank') <= 10) 

category_by_deparment.show(100)

### 3(2).store result to S3

category_by_deparment.coalesce(1).write.csv("s3a://data-hub-homework-data/analytics_result/category_by_deparment", header = True)


# COMMAND ----------

### join Transaction, Location and Product Table together for dashboard visualization
join1 = df_drop_dup.join(df_loc, df_drop_dup.store_location_key==df_loc.store_location_key, 'left') \
.select(df_drop_dup.store_location_key, df_drop_dup.product_key, df_drop_dup.collector_key, df_drop_dup.trans_dt, df_drop_dup.sales, df_drop_dup.units, df_drop_dup.trans_key, df_loc.province, df_loc.city) 

join2 = join1.join(df_prod, join1.product_key == df_prod.product_key, 'left') \
.select(join1.store_location_key, join1.product_key, join1.collector_key, join1.trans_dt, join1.sales, join1.units, join1.trans_key, join1.province, join1.city, df_prod.department, df_prod.category)

# COMMAND ----------

final_data = join2.withColumn("collector_key", col("collector_key").cast("double")) \
.withColumn("loyalty_type", when(col("collector_key") > 0, "Loyalty").otherwise("Non-loyalty")) \
.withColumn("units", col("units").cast("int")) \
.withColumn("sales", col("sales").cast("double")) 

# COMMAND ----------

### check the number of records 
final_data.count()

# COMMAND ----------

### upload to S3
final_data.coalesce(1).write.csv("s3a://data-hub-homework-data/dashboard-data/", header = True)

# COMMAND ----------

### check data types 
final_data.dtypes

# COMMAND ----------

