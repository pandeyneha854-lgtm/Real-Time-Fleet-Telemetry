# Import the os module to manage file system paths and directories.
import os
# Import the json module to serialize telemetry records to JSON.
import json
# Import the time module to control the 2-second generation interval.
import time
# Import the random module to generate mock telemetry values.
import random
# Import datetime to create ISO 8601 timestamps.
from datetime import datetime, timezone

# Define the output directory where JSON files will be dropped.
OUTPUT_DIR = "data/telemetry_stream"
# Define the interval in seconds between file writes.
WRITE_INTERVAL_SECONDS = 2
# Define a pool of mock truck IDs to simulate a fleet.
TRUCK_IDS = [f"TRUCK-{i:03d}" for i in range(1, 26)]

# Ensure the output directory exists before writing files.
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize a monotonically increasing file counter for unique filenames.
file_counter = 0

# Start an infinite loop to continuously emit telemetry records.
while True:
    # Select a random truck ID for this telemetry record.
    truck_id = random.choice(TRUCK_IDS)
    # Capture the current UTC time and format it as ISO 8601.
    timestamp = datetime.now(timezone.utc).isoformat()
    # Generate a realistic speed in km/h for a delivery truck.
    speed_kmh = round(random.uniform(20.0, 130.0), 2)
    # Generate a realistic engine temperature in Celsius.
    engine_temp_c = round(random.uniform(70.0, 115.0), 2)
    # Generate a simple GPS coordinate string (lat,lon).
    gps_coordinates = f"{round(random.uniform(25.0, 49.0), 6)},{round(random.uniform(-124.0, -67.0), 6)}"

    # Build the telemetry record as a Python dict.
    record = {
        "truck_id": truck_id,
        "timestamp": timestamp,
        "speed_kmh": speed_kmh,
        "engine_temp_c": engine_temp_c,
        "gps_coordinates": gps_coordinates,
    }

    # Build a unique filename using the counter and the current time in ms.
    filename = f"telemetry_{int(time.time() * 1000)}_{file_counter}.json"
    # Join the output directory with the filename to form a full file path.
    filepath = os.path.join(OUTPUT_DIR, filename)

    # Open the file in write mode.
    with open(filepath, "w", encoding="utf-8") as file_handle:
        # Serialize the record to JSON and write it to the file.
        json.dump(record, file_handle)

    # Increment the counter for the next file.
    file_counter += 1

    # Sleep to enforce the 2-second emission interval.
    time.sleep(WRITE_INTERVAL_SECONDS)
