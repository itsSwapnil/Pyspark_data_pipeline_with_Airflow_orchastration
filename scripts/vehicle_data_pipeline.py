from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, max, min, datediff, lit, to_date
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, lag
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Vehicle Data Pipeline") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-hadoop:8.5.2") \
    .getOrCreate()

with open("scripts/date.txt", "r") as f:
    process_date = f.read().strip()

print("Processing Date:", process_date)


vehicles_df = spark.read.csv("data/vehicles.csv", header=True, inferSchema=True)
usage_df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)
maintenance_df = spark.read.csv("data/maintenance.csv", header=True, inferSchema=True)

vehicles_df=vehicles_df.withColumn('created_date', to_date(vehicles_df['created_date'], 'dd-MM-yyyy'))
maintenance_df=maintenance_df.withColumn('date', to_date(maintenance_df['date'], 'dd-MM-yyyy'))


maint_agg = maintenance_df.groupBy("vehicle_id").agg(
    avg("cost").alias("avg_maint_cost"),
    count("date").alias("num_services"),
    max("date").alias("last_service_date")
)


vehicle_maint = vehicles_df.join(maint_agg, "vehicle_id", "left")

vehicle_maint = vehicle_maint.withColumn(
    "expected_next_service_date",
    expr("last_service_date + interval 180 days")
)

# Add expected expiry date (15 years from created date)
vehicle_maint = vehicle_maint.withColumn(
    "expected_expiry",
    add_months(col("created_date"), 15 * 12)
)

usage_agg = usage_df.groupBy("vehicle_id").agg(
    sum("kilometers").alias("total_kms"),
    sum("fuel_used_liters").alias("total_fuel")
)

# Join usage data
vehicle_full = vehicle_maint.join(usage_agg, "vehicle_id", "left")

vehicle_full = vehicle_full.withColumn(
    "fuel_per_km",
    when(col("total_kms") > 0, col("total_fuel") / col("total_kms")).otherwise(None)
)

vehicle_full = vehicle_full.withColumn(
    "expected_total_services",
    when(col("total_kms").isNotNull(), col("total_kms") / 10000)
    .otherwise(datediff(col("expected_expiry"), col("created_date")) / 180)
)


vehicle_full = vehicle_full.withColumn(
    "total_cost",
    col("purchase_cost") + (col("avg_maint_cost") * col("num_services"))
)

vehicle_full.show(truncate=False)

# Write to Elasticsearch (ELK)
vehicle_full.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "vehicles/_doc") \
    .option("es.nodes", "localhost") \
    .mode("overwrite") \
    .save("vehicle_usage_dashboard")