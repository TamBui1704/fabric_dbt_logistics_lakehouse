from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, row_number
from pyspark.sql.window import Window

def main():
    # LƯU Ý KỸ THUẬT FABRIC:
    # Trên Microsoft Fabric, 'spark.sql.parquet.vorder.enabled' MẶC ĐỊNH LÀ 'true'.
    # Do đó BẮT BUỘC phải set thủ công = 'false' ở tầng Silver để tắt V-Order, giúp tối ưu tốc độ xử lý ETL.
    spark = SparkSession.builder \
        .appName("Bronze to Silver Data Cleaning") \
        .config("spark.sql.parquet.vorder.enabled", "false") \
        .getOrCreate()
    
    bronze_lakehouse = "lh_bronze"
    silver_lakehouse = "lh_silver"

    print(f"Reading from '{bronze_lakehouse}' -> Writing to '{silver_lakehouse}'")

    print("Cleaning Dimensions...")
    
    # Clean Customers
    df_customers = spark.table(f"{bronze_lakehouse}.dbo.bronze_customers") \
        .filter(col("customer_id").isNotNull()) \
        .withColumn("customer_id", trim(upper(col("customer_id")))) \
        .withColumn("customer_name", trim(col("customer_name"))) \
        .withColumn("customer_type", trim(upper(col("customer_type")))) \
        .dropDuplicates(["customer_id"])
    df_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_lakehouse}.dbo.silver_customers")

    # Clean Services
    df_services = spark.table(f"{bronze_lakehouse}.dbo.bronze_services") \
        .filter(col("service_id").isNotNull()) \
        .withColumn("service_id", trim(upper(col("service_id")))) \
        .withColumn("service_name", trim(col("service_name"))) \
        .withColumn("service_group", trim(col("service_group"))) \
        .dropDuplicates(["service_id"])
    df_services.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_lakehouse}.dbo.silver_services")

    # Clean POS Locations
    df_pos = spark.table(f"{bronze_lakehouse}.dbo.bronze_pos_locations") \
        .filter(col("pos_id").isNotNull()) \
        .withColumn("pos_id", trim(upper(col("pos_id")))) \
        .withColumn("pos_name", trim(col("pos_name"))) \
        .withColumn("province", trim(col("province"))) \
        .withColumn("region", trim(col("region"))) \
        .dropDuplicates(["pos_id"])
    df_pos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_lakehouse}.dbo.silver_pos_locations")

    print("Cleaning Facts (Deterministic Window Deduplication & Data Quality Filters)...")

    # Clean Revenue Fact with Window Deduplication & Type Casting
    window_revenue = Window.partitionBy("shipment_id").orderBy(col("created_at").desc())
    df_revenue = spark.table(f"{bronze_lakehouse}.dbo.bronze_revenue") \
        .filter(col("shipment_id").isNotNull() & col("created_at").isNotNull()) \
        .withColumn("shipment_id", trim(upper(col("shipment_id")))) \
        .withColumn("customer_id", trim(upper(col("customer_id")))) \
        .withColumn("service_id", trim(upper(col("service_id")))) \
        .withColumn("origin_pos_id", trim(upper(col("origin_pos_id")))) \
        .withColumn("dest_pos_id", trim(upper(col("dest_pos_id")))) \
        .withColumn("revenue_amount", col("revenue_amount").cast("decimal(18,2)")) \
        .withColumn("weight_kg", col("weight_kg").cast("double")) \
        .withColumn("rn", row_number().over(window_revenue)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    df_revenue.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("partition_ym").saveAsTable(f"{silver_lakehouse}.dbo.silver_revenue")

    # Clean Delivery Fact with Window Deduplication & Type Casting
    window_delivery = Window.partitionBy("shipment_id").orderBy(col("delivered_at").desc())
    df_delivery = spark.table(f"{bronze_lakehouse}.dbo.bronze_delivery") \
        .filter(col("shipment_id").isNotNull() & col("delivered_at").isNotNull()) \
        .withColumn("shipment_id", trim(upper(col("shipment_id")))) \
        .withColumn("delivery_pos_id", trim(upper(col("delivery_pos_id")))) \
        .withColumn("service_id", trim(upper(col("service_id")))) \
        .withColumn("delivery_fee", col("delivery_fee").cast("decimal(18,2)")) \
        .withColumn("rn", row_number().over(window_delivery)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    df_delivery.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("partition_ym").saveAsTable(f"{silver_lakehouse}.dbo.silver_delivery")

    print("Done cleaning data from Bronze to Silver Lakehouse successfully!")

if __name__ == "__main__":
    main()

