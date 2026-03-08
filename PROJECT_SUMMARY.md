# Real-Time Fleet Telemetry (DP-600 Streaming & Lakehouse)

## Overview
This project simulates real-time IoT telemetry from a fleet of delivery trucks and processes it with PySpark Structured Streaming. It demonstrates event-time windowing, watermarking for late data, and writing aggregated results to a Delta Lakehouse path compatible with Microsoft Fabric.

## What was built
- `stream_simulator.py` continuously generates JSON telemetry files every 2 seconds to simulate an Eventstream or drop-folder source.
- `pyspark_streaming.py` reads the JSON files as a streaming source with an explicit schema, applies event-time windowing and watermarking, computes rolling metrics, and writes results to a Delta table path in append mode with checkpointing.

## Telemetry Schema
Each event contains:
- `truck_id` (string)
- `timestamp` (ISO 8601 string)
- `speed_kmh` (float)
- `engine_temp_c` (float)
- `gps_coordinates` (string, "lat,lon")

## Streaming Logic
- Ingest JSON files with an explicit `StructType` schema (no inference).
- Convert the timestamp string to Spark `TimestampType` and use it as event time.
- Apply a 2-minute watermark for late data handling.
- Aggregate over a 5-minute tumbling window per `truck_id`.
- Compute:
  - `average_speed`
  - `max_engine_temp`
- Flag critical alerts when:
  - `average_speed` > 110 km/h OR
  - `max_engine_temp` > 105 °C

## Output Sink
- Writes to Delta format at `lakehouse/tables/truck_telemetry_agg`.
- Uses `append` mode for windowed aggregates with watermarking.
- Uses `checkpointLocation` at `lakehouse/checkpoints/truck_telemetry_agg`.
- Checkpointing persists streaming progress and state so Spark can recover from notebook crashes without data loss or duplicate processing.

## How to run (local simulation)
1. Start the simulator:
   - `python stream_simulator.py`
2. Start the PySpark streaming job (Fabric notebook or local Spark):
   - `spark-submit pyspark_streaming.py`

## Files
- `stream_simulator.py`
- `pyspark_streaming.py`
- `PROJECT_SUMMARY.md`
