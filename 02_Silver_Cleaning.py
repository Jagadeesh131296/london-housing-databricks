# Databricks notebook source
from pyspark.sql.functions import col, to_date, upper, current_timestamp

# 1. Read from your Bronze table
bronze_df = spark.read.table("london_data.real_estate.bronze_property_sales")

# 2. Transform the data
silver_df = (bronze_df
    # Rename columns to be more descriptive (if not done in Bronze)
    .withColumnRenamed("district", "borough")
    
    # Cast Data Types: Prices should be Integers, Dates should be Date format
    .withColumn("sale_price", col("price").cast("int"))
    .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd HH:mm"))
    
    # Cleaning: Standardize text to uppercase for consistent joining later
    .withColumn("county", upper(col("county")))
    .withColumn("city", upper(col("town_city")))
    
    # Filter: Keep only London records and remove rows with missing prices
    .filter((col("city") == "LONDON") | (col("borough") == "GREATER LONDON"))
    .filter(col("sale_price").isNotNull())
    
    # Metadata: Add a timestamp to know when this row was processed
    .withColumn("ingestion_timestamp", current_timestamp())
    
    # Select only the columns we need for analysis
    .select("transaction_id", "sale_price", "sale_date", "postcode", "borough", "street", "ingestion_timestamp")
)

# 3. Write as the Silver Delta Table
silver_df.write.mode("overwrite").saveAsTable("london_data.real_estate.silver_property_sales")

print("Silver table 'silver_property_sales' successfully created.")
display(silver_df.limit(5))