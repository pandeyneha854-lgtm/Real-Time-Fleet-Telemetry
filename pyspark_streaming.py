# Import SparkSession to initialize the Spark application.
from pyspark.sql import SparkSession
# Import types to define an explicit schema for incoming JSON.
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# Import functions for timestamp casting, windowing, and aggregations.
from pyspark.sql.functions import col, to_timestamp, window, avg, max as max_

# Create or get the existing Spark session for Structured Streaming.
spark = SparkSession.builder.appName("FleetTelemetryStreaming").getOrCreate()

# Define the input path that the simulator writes JSON files into.
INPUT_PATH = "data/telemetry_stream"
# Define the output Delta table path for aggregated results.
OUTPUT_PATH = "lakehouse/tables/truck_telemetry_agg"
# Define the checkpoint path required for reliable streaming.
CHECKPOINT_PATH = "lakehouse/checkpoints/truck_telemetry_agg"

# Define an explicit schema to enforce data quality on read.
schema = StructType(
    [
        # Truck identifier as a string.
        StructField("truck_id", StringType(), True),
        # ISO 8601 timestamp as a string (will be cast to timestamp later).
        StructField("timestamp", StringType(), True),
        # Speed in km/h as a double.
        StructField("speed_kmh", DoubleType(), True),
        # Engine temperature in Celsius as a double.
        StructField("engine_temp_c", DoubleType(), True),
        # GPS coordinate string as "lat,lon".
        StructField("gps_coordinates", StringType(), True),
    ]
)

# Read the JSON files as a streaming source using the explicit schema.
raw_stream = (
    spark.readStream
    # Specify JSON as the input format.
    .format("json")
    # Apply the schema to prevent inference issues.
    .schema(schema)
    # Set the input path for the file stream.
    .load(INPUT_PATH)
)

# Cast the timestamp string to Spark's TimestampType for event-time processing.
stream_with_event_time = raw_stream.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

# Apply watermarking to handle late data and windowing for 5-minute tumbling windows.
windowed_agg = (
    stream_with_event_time
    # Allow data to arrive up to 2 minutes late.
    .withWatermark("event_time", "2 minutes")
    # Group by truck_id and 5-minute tumbling window on event_time.
    .groupBy(
        col("truck_id"),
        window(col("event_time"), "5 minutes")
    )
    # Compute average speed and max engine temperature per window.
    .agg(
        avg(col("speed_kmh")).alias("average_speed"),
        max_(col("engine_temp_c")).alias("max_engine_temp")
    )
)

# Add a boolean alert flag based on the thresholds provided.
alerts = windowed_agg.withColumn(
    "is_critical_alert",
    (col("average_speed") > 110.0) | (col("max_engine_temp") > 105.0)
)

# Write the aggregated results to a Delta table in append mode.
query = (
    alerts.writeStream
    # Use Delta format for Lakehouse compatibility.
    .format("delta")
    # Use append mode for windowed aggregations with watermarking.
    .outputMode("append")
    # Set a checkpoint location to store streaming progress and state.
    # Checkpointing lets Spark recover the exact offsets and state if a Fabric
    # notebook crashes or restarts, preventing duplicate processing or data loss.
    .option("checkpointLocation", CHECKPOINT_PATH)
    # Set the output path for the Delta table.
    .start(OUTPUT_PATH)
)

# Keep the stream running until terminated.
query.awaitTermination()
