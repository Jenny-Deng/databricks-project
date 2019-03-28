# Databricks notebook source
ACCESS_KEY = "xxxxxxx"
SECRET_KEY = "xxxxxxxxxxxxxxxxxxxxx"

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

### replace null values with zeros 
df_fill_na = df_sum.fillna({'sales': '0', 'units': '0'})

### drop duplicates and check number of records
from pyspark.sql.functions import *

df_drop_dup = df_fill_na.drop_duplicates() 
df_drop_dup.count()

### convert data types 
df_trans = df_drop_dup.withColumn("units", col("units").cast("int")) \
.withColumn("sales", col("sales").cast("double")) \
.withColumn("collector_key", col("collector_key").cast("double"))

# COMMAND ----------

### convert dataframe to table 

df_trans.createOrReplaceTempView("tab_trans")
df_loc.createOrReplaceTempView("tab_loc")
df_prod.createOrReplaceTempView("tab_prod")

# COMMAND ----------

df_trans_loc = spark.sql("""SELECT a.store_location_key, a.sales, b.province, b.store_num 
                FROM tab_trans as a 
                left join tab_loc as b 
                on a.store_location_key = b.store_location_key""")

# COMMAND ----------

spark.sql("""select store_num, round(sum(sales), 2) as total_sales_by_store 
from tab_trans_loc 
group by store_num 
order by total_sales_by_store desc""")

# COMMAND ----------

### 1(1).which provinces and stores are performing well 

df_trans_loc.createOrReplaceTempView("tab_trans_loc")

spark.sql("""select store_num, round(sum(sales), 2) as total_sales_by_store 
from tab_trans_loc 
group by store_num 
order by total_sales_by_store desc""").show(5)

spark.sql("""select province, round(sum(sales), 2) as total_sales_by_province 
from tab_trans_loc 
group by province 
order by total_sales_by_province desc""").show(5)       

# COMMAND ----------

### 1(2).top stores in each province performing compared with the average store of the province.

spark.sql("""select province, round(max(sales), 2) as max_sales_by_province, round(avg(sales), 2) as avg_sales_by_province, round(max(sales)-avg(sales) ,2) as diff_sales_by_province
    from tab_trans_loc 
    group by province 
    order by diff_sales_by_province desc""").show()

# COMMAND ----------

### 2(1).customers in the loyalty program performing compared to non-loyalty customers 
spark.sql("""select loyalty_type, round(sum(sales), 2) as total_sales_by_loyalty 
from (select *, 
case when collector_key > 0 then "Loyalty" 
else "Non-loyalty" end as loyalty_type 
from tab_trans) 
group by loyalty_type 
order by total_sales_by_loyalty""").show()

# COMMAND ----------

### left join transaction table with product table - grab column department and category
df_trans_prod = spark.sql("""SELECT a.product_key, a.sales, c.department, c.category 
                FROM tab_trans as a 
                left join tab_prod as c 
                on a.product_key = c.product_key""")

df_trans_prod.createOrReplaceTempView("tab_trans_prod")

### 2(2).what category of products is contributing to most of ACME’s sales
spark.sql("""SELECT category, round(sum(sales), 2) as total_sales_by_category
                FROM tab_trans_prod
                where category != "null"
                group by category
                order by total_sales_by_category desc""").show(5)

# COMMAND ----------

### 3(1). top 5 stores by province

spark.sql(""" 
select *
from(
select *, RANK() OVER(PARTITION BY province ORDER BY total_sales desc) rnk
from(
select province, store_num, round(sum(sales), 2) as total_sales
from tab_trans_loc
group by province, store_num)
) as A
where A.rnk <= 5
""").show()

# COMMAND ----------

### 3(2). top 10 product categories by department
spark.sql(""" 
select *
from(
select *, RANK() OVER(PARTITION BY department ORDER BY total_sales desc) rnk
from(
select department, category, round(sum(sales), 2) as total_sales
from tab_trans_prod
group by department, category)
) as B
where B.rnk <= 10
""").show()
