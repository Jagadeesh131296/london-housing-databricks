# Databricks notebook source
# MAGIC %sql
# MAGIC -- 1. Create the Schema (the sub-folder for your project)
# MAGIC CREATE SCHEMA IF NOT EXISTS london_data.real_estate;
# MAGIC
# MAGIC -- 2. Create the Volume (the place to hold your CSV file)
# MAGIC CREATE VOLUME IF NOT EXISTS london_data.real_estate.property_files;

# COMMAND ----------

# 1. Path to your uploaded CSV in the Volume
csv_path = "/Volumes/london_data/real_estate/property_files/London_2025_house_price.csv"

# 2. Define the schema
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("sale_date", StringType(), True),   # will convert to DateType later
    StructField("postcode", StringType(), True),
    StructField("property_type", StringType(), True),
    StructField("new_build", StringType(), True),
    StructField("tenure", StringType(), True),
    StructField("paon", StringType(), True),
    StructField("saon", StringType(), True),
    StructField("street", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("town_city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("county", StringType(), True),
    StructField("ppd_category", StringType(), True),
    StructField("record_status", StringType(), True)
])

# 3. Read the CSV using the schema we defined earlier
df_raw = spark.read.format("csv").schema(schema).option("header", "true").load(csv_path)

# 4. Write it as a Delta Table in your Catalog


df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("london_data.real_estate.bronze_property_sales")


# print("Bronze table created successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE london_data.real_estate.bronze_property_sales