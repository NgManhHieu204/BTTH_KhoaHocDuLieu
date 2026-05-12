from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time

# Ghi nhận thời gian bắt đầu
start_time = time.time()

# KHỞI TẠO SPARK SESSION KẾT NỐI VỚI CLUSTER
spark = SparkSession.builder \
    .appName("NYCTaxi_Distributed_Analysis") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.cores.max", "4") \
    .getOrCreate()

# ĐỌC DỮ LIỆU (Lazy Evaluation)
folder_path = "/opt/spark-data/*.parquet" 
sdf = spark.read.parquet(folder_path)

# LÀM SẠCH VÀ CHUYỂN ĐỔI
sdf = sdf.withColumn(
    "trip_duration_mins",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
)
sdf = sdf.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))

# TỔNG HỢP DỮ LIỆU
hourly_stats_spark = sdf.groupBy("pickup_hour").agg(
    F.mean("total_amount").alias("avg_revenue"),
    F.mean("trip_duration_mins").alias("avg_duration"),
    F.count("*").alias("total_trips")
).orderBy("pickup_hour")

# 5. THỰC THI
hourly_stats_spark.show(24)

# Ghi nhận thời gian kết thúc
end_time = time.time()
print(f"\nThời gian xử lý bằng PySpark (Cluster 2 Workers): {end_time - start_time:.2f} giây")

# Dùng session giải phóng tài nguyên
spark.stop()