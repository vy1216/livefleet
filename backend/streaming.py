import pathway as pw
import psycopg2
import pandas as pd
import time
import threading
from datetime import datetime
import os

# --------------------------------------------------
# Database Connection
# --------------------------------------------------

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
# --------------------------------------------------
# Schema
# --------------------------------------------------

class TruckSchema(pw.Schema):
    truck_id: str
    lat: float
    lon: float
    destination_lat: float
    destination_lon: float
    speed: float
    temperature: float
    timestamp: str

# --------------------------------------------------
# Read Streaming CSV
# --------------------------------------------------

source = pw.io.csv.read(
    "stream_data.csv",
    schema=TruckSchema,
    mode="streaming"
)

# --------------------------------------------------
# Distance Calculation
# --------------------------------------------------

with_distance = source.select(
    *pw.this,
    distance_km=(
        (
            (pw.this.destination_lat - pw.this.lat) ** 2 +
            (pw.this.destination_lon - pw.this.lon) ** 2
        ) ** 0.5
    ) * 111
)

# --------------------------------------------------
# ETA Calculation (Safe)
# --------------------------------------------------

with_eta = with_distance.select(
    *pw.this,
    eta_minutes=pw.if_else(
        pw.this.speed > 0,
        (pw.this.distance_km / pw.this.speed) * 60,
        9999
    )
)

# --------------------------------------------------
# AI Risk + Anomaly Engine
# --------------------------------------------------

risk_engine = with_eta.select(
    truck_id=pw.this.truck_id,
    lat=pw.this.lat,
    lon=pw.this.lon,
    destination_lat=pw.this.destination_lat,
    destination_lon=pw.this.destination_lon,
    speed=pw.this.speed,
    temperature=pw.this.temperature,
    eta_minutes=pw.this.eta_minutes,
    timestamp=pw.this.timestamp,

    # Risk score calculation (clean)
    risk_score=
        (pw.if_else(pw.this.temperature > 8, 30, 0)) +
        (pw.if_else(pw.this.temperature < 2, 25, 0)) +
        (pw.if_else(pw.this.speed > 70, 25, 0)) +
        (pw.if_else(pw.this.speed < 15, 20, 0)) +
        (pw.if_else(pw.this.eta_minutes > 20, 40, 0)),

    # AI Anomaly detection
    anomaly_flag=
        pw.if_else(
            (pw.this.temperature > 10) |
            (pw.this.temperature < 1) |
            (pw.this.speed > 80) |
            (pw.this.speed < 5),
            True,
            False
        )
)

# --------------------------------------------------
# Final Risk Alerts Table
# --------------------------------------------------

risk_table = risk_engine.select(
    truck_id=pw.this.truck_id,
    risk_score=pw.this.risk_score,
    risk_level=pw.if_else(
        pw.this.risk_score > 70,
        "CRITICAL",
        pw.if_else(
            pw.this.risk_score > 40,
            "WARNING",
            "NORMAL"
        )
    ),
    anomaly=pw.this.anomaly_flag,
    time=pw.this.timestamp
)

# --------------------------------------------------
# Write to CSV Buffers
# --------------------------------------------------

pw.io.csv.write(with_eta, "fleet_buffer.csv")
pw.io.csv.write(risk_table, "risk_buffer.csv")

# --------------------------------------------------
# Background DB Sync Thread
# --------------------------------------------------

def db_sync():
    while True:
        try:
            if not pd.io.common.file_exists("fleet_buffer.csv"):
                time.sleep(3)
                continue

            fleet_df = pd.read_csv("fleet_buffer.csv")
            risk_df = pd.read_csv("risk_buffer.csv")

            conn = get_connection()
            cur = conn.cursor()

            # Insert Fleet Data
            for _, row in fleet_df.tail(5).iterrows():
                cur.execute("""
                    INSERT INTO fleet_data
                    (truck_id, lat, lon, destination_lat,
                     destination_lon, speed, temperature, eta_minutes)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    row["truck_id"],
                    row["lat"],
                    row["lon"],
                    row["destination_lat"],
                    row["destination_lon"],
                    row["speed"],
                    row["temperature"],
                    row["eta_minutes"]
                ))

            # Insert Risk Data
            for _, row in risk_df.tail(5).iterrows():
                if row["risk_level"] != "NORMAL":
                    cur.execute("""
                        INSERT INTO risk_alerts
                        (truck_id, risk_score, risk_level, anomaly, time)
                        VALUES (%s,%s,%s,%s,%s)
                    """, (
                        row["truck_id"],
                        row["risk_score"],
                        row["risk_level"],
                        row["anomaly"],
                        int(datetime.now().timestamp())
                    ))

            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            print("DB Sync Error:", e)

        time.sleep(5)

# Start background thread
threading.Thread(target=db_sync, daemon=True).start()

# --------------------------------------------------

if __name__ == "__main__":
    pw.run()