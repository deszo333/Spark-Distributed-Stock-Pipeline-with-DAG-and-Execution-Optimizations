from pyspark.sql import SparkSession
from pyspark.sql.functions import col, randn, date_add

# 1. Initialize Spark (using all your local CPU cores)
spark = SparkSession.builder \
    .appName("SP500_BigData_Scaler") \
    .master("local[*]") \
    .getOrCreate()

print("Loading base dataset...")

# 2. Read your specific downloaded CSV
df_base = spark.read.csv("data/raw/SP500_Historical_Data.csv", header=True, inferSchema=True)

# 3. Standardize the column names to match our architecture
df_base = df_base.withColumnRenamed("Ticker", "symbol") \
                 .withColumnRenamed("Date", "timestamp")

print("Multiplying data to reach 1GB+ (27 Million Rows)...")

# 4. The Multiplier Loop (x10)
big_data_df = df_base
for i in range(1, 10):
    # Duplicate, shift the dates forward, and add slight noise to the Close price
    noisy_df = df_base.withColumn("timestamp", date_add(col("timestamp"), i * 365)) \
                      .withColumn("Close", col("Close") + (randn() * 0.5))
    
    big_data_df = big_data_df.union(noisy_df)

print("Writing 1GB+ dataset to Parquet... This will take a few minutes...")

# 5. Save the massive dataset partitioned by symbol
big_data_df.write.mode("overwrite").partitionBy("symbol").parquet("data/processed/historical_parquet")

print("Data generation complete! Ready for ML and MPI Benchmarks.")
spark.stop()