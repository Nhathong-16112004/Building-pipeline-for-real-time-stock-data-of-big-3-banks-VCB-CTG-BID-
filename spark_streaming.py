from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, avg, max as max_, min as min_,
    stddev, count, to_timestamp, from_json, when
)
from pyspark.sql.types import StructType, StringType, DoubleType

# Tạo Spark session
spark = SparkSession.builder \
    .appName("BankPriceAnalysis_NoSliding_Full") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setLogLevel("ERROR")

# Đọc dữ liệu từ Kafka
df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bank_prices") \
    .option("startingOffsets", "earliest") \
    .load()

# Schema JSON
schema = StructType() \
    .add("symbol", StringType()) \
    .add("ticker", StringType()) \
    .add("current_price", DoubleType()) \
    .add("volume", DoubleType()) \
    .add("pe_ratio", DoubleType()) \
    .add("time", StringType())

# Parse JSON + chuyển kiểu thời gian
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

df_parsed = df_parsed.withColumn("event_time", to_timestamp("time"))

# Gộp dữ liệu theo cửa sổ 10 phút
agg_df = df_parsed.groupBy(
    window(col("event_time"), "10 minutes"),
    col("symbol"), col("ticker")
).agg(
    avg("current_price").alias("avg_price"),
    max_("current_price").alias("max_price"),
    min_("current_price").alias("min_price"),
    stddev("current_price").alias("volatility"),    
    avg("volume").alias("avg_volume"),
    stddev("volume").alias("volume_volatility"),   
    avg("pe_ratio").alias("avg_pe"),
    count("*").alias("record_count")
)

# Tính % thay đổi giá trong chính cửa sổ 10 phút đó
agg_df = agg_df.withColumn(
    "price_change_pct",
    when(
        (col("min_price").isNotNull()) & (col("min_price") != 0),
        (col("max_price") - col("min_price")) / col("min_price") * 100
    )
)

# Chọn cột cuối cùng
final_df = agg_df.select(
    col("symbol"),
    col("ticker"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("avg_price"),
    col("price_change_pct"),
    col("max_price"),
    col("min_price"),
    col("volatility"),
    col("avg_volume"),
    col("volume_volatility"),
    col("avg_pe"),
    col("record_count")
)

# Hàm ghi từng batch vào PostgreSQL
def write_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[Batch {batch_id}] Không có dữ liệu, skip.")
        return

    jdbc_url = "jdbc:postgresql://postgres:5432/stockdb"
    props = {
        "user": "spark_user",
        "password": "123456",
        "driver": "org.postgresql.Driver"
    }

    print(f"\n===== BATCH {batch_id} =====")
    batch_df.show(truncate=False)

    (batch_df.write
        .mode("append")
        .jdbc(jdbc_url, "stock_aggregates", properties=props))

    row_count = batch_df.count()
    print("Batch", batch_id, "da ghi", row_count, "dong vao PostgreSQL")

# Chạy streaming: vừa ghi DB vừa có thể xem log
query = final_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "./checkpoints/bank_prices") \
    .trigger(processingTime="20 seconds") \
    .start()

query.awaitTermination()
