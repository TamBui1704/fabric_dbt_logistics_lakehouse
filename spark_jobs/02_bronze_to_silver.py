from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper

def main():
    spark = SparkSession.builder.appName("Bronze to Silver Data Cleaning").getOrCreate()
    
    bronze_lakehouse = "lh_bronze"
    silver_lakehouse = "lh_silver"

    print(f"Lưu ý: Đảm bảo bạn đã tạo Lakehouse '{bronze_lakehouse}' và '{silver_lakehouse}'.")

    print("Cleaning Dimensions...")
    
    # Clean Customers
    df_customers = spark.table(f"{bronze_lakehouse}.bronze_customers") \
        .withColumn("customer_id", trim(upper(col("customer_id")))) \
        .dropDuplicates(["customer_id"])
    df_customers.write.format("delta").mode("overwrite").saveAsTable(f"{silver_lakehouse}.silver_customers")

    # Clean Services
    df_services = spark.table(f"{bronze_lakehouse}.bronze_services") \
        .withColumn("service_id", trim(upper(col("service_id")))) \
        .dropDuplicates(["service_id"])
    df_services.write.format("delta").mode("overwrite").saveAsTable(f"{silver_lakehouse}.silver_services")

    # Clean POS Locations
    df_pos = spark.table(f"{bronze_lakehouse}.bronze_pos_locations") \
        .withColumn("pos_id", trim(upper(col("pos_id")))) \
        .dropDuplicates(["pos_id"])
    df_pos.write.format("delta").mode("overwrite").saveAsTable(f"{silver_lakehouse}.silver_pos_locations")

    print("Cleaning Facts...")

    # Clean Revenue
    df_revenue = spark.table(f"{bronze_lakehouse}.bronze_revenue") \
        .withColumn("shipment_id", trim(upper(col("shipment_id")))) \
        .dropDuplicates(["shipment_id"])
    df_revenue.write.format("delta").mode("overwrite").saveAsTable(f"{silver_lakehouse}.silver_revenue")

    # Clean Delivery
    df_delivery = spark.table(f"{bronze_lakehouse}.bronze_delivery") \
        .withColumn("shipment_id", trim(upper(col("shipment_id")))) \
        .dropDuplicates(["shipment_id"])
    df_delivery.write.format("delta").mode("overwrite").saveAsTable(f"{silver_lakehouse}.silver_delivery")

    print("Done cleaning data from Bronze to Silver Lakehouse!")

if __name__ == "__main__":
    main()
