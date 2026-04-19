import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, round as _round, substring

# 1. Standard Windows Hadoop Setup (Required)
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['hadoop.home.dir'] = "C:/hadoop"
hadoop_bin = "C:/hadoop/bin"
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
sys.path.append(hadoop_bin)

# 2. Start a Batch Spark Session 
print("Starting Batch ETL Job...")
spark = SparkSession.builder \
    .appName("GoldLayerETL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 3. EXTRACT: Read all the raw JSON files from the Data Lake
print("Extracting raw data from Data Lake...")
raw_data_df = spark.read.json("./data_lake/sales_data/*")

# ---------------------------------------------------------
# 🌟 NEW FEATURE: TIME WINDOWS (Velocity Tracking)
# ---------------------------------------------------------
print("Transforming data (Calculating Revenue per Minute)...")
# We extract characters 12 through 16 from "YYYY-MM-DD HH:MM:SS" to get "HH:MM"
gold_layer_df = raw_data_df \
    .withColumn("Minute", substring("timestamp", 12, 5)) \
    .groupBy("Minute", "product") \
    .agg(
        count("order_id").alias("Total_Orders"),
        _round(_sum("price"), 2).alias("Total_Revenue")
    ) \
    .orderBy("Minute", ascending=True)

# Show a quick preview in the console
print("\n--- FINAL REPORT PREVIEW ---")
gold_layer_df.show()

# 5. LOAD: Save the final table as a single CSV file for the Business Team
print("Loading final report to Data Warehouse...")
gold_layer_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("./data_warehouse/final_report")

print("Batch ETL Job Complete!")
spark.stop()
