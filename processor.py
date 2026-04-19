import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# 1. Force the Hadoop environment variables
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['hadoop.home.dir'] = "C:/hadoop"
hadoop_bin = "C:/hadoop/bin"
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
sys.path.append(hadoop_bin)

print(f"HADOOP_HOME is set to: {os.environ['HADOOP_HOME']}")

# 2. Start Spark (and tell it to download the Kafka plugin automatically)
print("Starting Spark Processor... ")
spark = SparkSession.builder \
    .appName("FlashSaleProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()

# Reduce background warning messages
spark.sparkContext.setLogLevel("ERROR") 
print("Spark Started Successfully!")

# 3. Tell Spark what our data looks like (Must match the producer!)
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# 4. Read the live stream from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Clean the data (Convert from raw Kafka bytes to a JSON table)
parsed_df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# ---------------------------------------------------------
# 🌟 NEW FEATURE: FRAUD DETECTION BRANCH (Data Routing)
# ---------------------------------------------------------
# We split the data stream. Anything over $2500 gets flagged.
fraud_df = parsed_df.filter(col("price") >= 2500.0) 
normal_df = parsed_df 

# The Analytics! (Calculate Total Revenue per Product for the console)
revenue_df = parsed_df \
    .groupBy("product") \
    .agg(_sum("price").alias("Total_Revenue"))

# 6. Save the normal data to the standard Data Lake folder
print("Writing live data to our Local Data Lake...")
file_query = normal_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "./data_lake/sales_data") \
    .option("checkpointLocation", "./checkpoints/sales_data") \
    .start()

# 7. Route the suspicious data to the Fraud Alerts folder
print("Routing high-value transactions to Fraud Alerts folder...")
fraud_query = fraud_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "./data_lake/fraud_alerts") \
    .option("checkpointLocation", "./checkpoints/fraud_alerts") \
    .start()

# 8. Keep the live dashboard running in the console
print("Starting live console dashboard...")
console_query = revenue_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# 9. Tell Spark to run ALL streams at the same time until we stop it
spark.streams.awaitAnyTermination()