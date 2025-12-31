# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create the Gold Table
# MAGIC CREATE OR REPLACE TABLE london_data.real_estate.gold_borough_stats AS
# MAGIC SELECT 
# MAGIC     borough,
# MAGIC     count(*) AS total_sales,
# MAGIC     round(avg(sale_price), 0) AS average_price,
# MAGIC     min(sale_price) AS cheapest_sale,
# MAGIC     max(sale_price) AS most_expensive_sale
# MAGIC FROM london_data.real_estate.silver_property_sales
# MAGIC GROUP BY borough
# MAGIC ORDER BY average_price DESC;
# MAGIC
# MAGIC -- View the results
# MAGIC SELECT * FROM london_data.real_estate.gold_borough_stats;