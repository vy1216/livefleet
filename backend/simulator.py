import random
import time
import csv
from datetime import datetime

FILE_PATH = "stream_data.csv"

# --------------------------------------------------
# Truck initial positions + destinations
# --------------------------------------------------

TRUCKS = {
    "T1": {
        "lat": 28.50,
        "lon": 77.20,
        "destination_lat": 28.70,
        "destination_lon": 77.30
    },
    "T2": {
        "lat": 28.55,
        "lon": 77.25,
        "destination_lat": 28.80,
        "destination_lon": 77.40
    },
    "T3": {
        "lat": 28.60,
        "lon": 77.22,
        "destination_lat": 28.90,
        "destination_lon": 77.50
    },
}

# --------------------------------------------------
# Move truck slightly toward destination
# --------------------------------------------------

def move_towards(current, destination, step=0.001):
    if current < destination:
        return current + step
    elif current > destination:
        return current - step
    return current

# --------------------------------------------------
# Write CSV Header (with destination fields)
# --------------------------------------------------

def write_header():
    with open(FILE_PATH, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "truck_id",
            "lat",
            "lon",
            "destination_lat",
            "destination_lon",
            "speed",
            "temperature",
            "timestamp"
        ])

# --------------------------------------------------
# Append new streaming row
# --------------------------------------------------

def append_row(row):
    with open(FILE_PATH, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(row)

# --------------------------------------------------
# Main Streaming Loop
# --------------------------------------------------

def run():
    write_header()
    print("🚛 Writing streaming data to CSV...")

    while True:
        for truck_id, truck_data in TRUCKS.items():

            # Move toward destination
            truck_data["lat"] = move_towards(
                truck_data["lat"],
                truck_data["destination_lat"]
            )
            truck_data["lon"] = move_towards(
                truck_data["lon"],
                truck_data["destination_lon"]
            )

            speed = random.uniform(30, 60)
            temperature = random.uniform(2, 10)

            row = [
                truck_id,
                round(truck_data["lat"], 6),
                round(truck_data["lon"], 6),
                truck_data["destination_lat"],
                truck_data["destination_lon"],
                round(speed, 2),
                round(temperature, 2),
                datetime.now().isoformat()
            ]

            append_row(row)
            print(row)

        time.sleep(2)

# --------------------------------------------------

if __name__ == "__main__":
    run()