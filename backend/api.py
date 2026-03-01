from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import pandas as pd
import os

# --------------------------------------------------
# App Initialization
# --------------------------------------------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------
# Database Connection
# --------------------------------------------------

def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL not set in environment variables")
    return psycopg2.connect(database_url)

# --------------------------------------------------
# Routes
# --------------------------------------------------

@app.get("/")
def root():
    return {"message": "LiveFleet API running (Postgres Mode)"}

from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/dashboard")
def serve_dashboard():
    return FileResponse("frontend/index.html")

# --------------------------------------------------
# Fleet Endpoint
# --------------------------------------------------

@app.get("/fleet")
def get_fleet():
    try:
        conn = get_connection()

        df = pd.read_sql("""
            SELECT *
            FROM fleet_data
            ORDER BY timestamp DESC
        """, conn)

        conn.close()

        if df.empty:
            return []

        # Keep latest record per truck
        df = df.sort_values("timestamp", ascending=False)
        df = df.drop_duplicates(subset=["truck_id"], keep="first")

        return df.to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}

# --------------------------------------------------
# Risk Endpoint
# --------------------------------------------------

@app.get("/risk")
def get_risk():
    try:
        conn = get_connection()

        df = pd.read_sql("""
            SELECT truck_id, risk_score, risk_level, time
            FROM risk_alerts
            WHERE risk_level != 'NORMAL'
        """, conn)

        conn.close()

        if df.empty:
            return []

        df = df.sort_values("time", ascending=False)
        df = df.drop_duplicates(subset=["truck_id"], keep="first")

        severity_order = {"CRITICAL": 2, "WARNING": 1}
        df["severity_rank"] = df["risk_level"].map(severity_order)
        df = df.sort_values("severity_rank", ascending=False)

        return df[["truck_id", "risk_score", "risk_level", "time"]].to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}



@app.get("/stats")
def get_stats():
    try:
        conn = get_connection()

        df = pd.read_sql("SELECT * FROM fleet_data", conn)
        conn.close()

        if df.empty:
            return {
                "total_trucks": 0,
                "avg_speed": 0,
                "avg_temperature": 0
            }

        return {
            "total_trucks": df["truck_id"].nunique(),
            "avg_speed": round(df["speed"].mean(), 2),
            "avg_temperature": round(df["temperature"].mean(), 2)
        }

    except Exception as e:
        return {"error": str(e)}
    

@app.get("/risk-summary")
def risk_summary():
    try:
        conn = get_connection()

        df = pd.read_sql("""
            SELECT risk_level, COUNT(*) as count
            FROM risk_alerts
            GROUP BY risk_level
        """, conn)

        conn.close()

        summary = {
            "CRITICAL": 0,
            "WARNING": 0
        }

        for _, row in df.iterrows():
            if row["risk_level"] in summary:
                summary[row["risk_level"]] = int(row["count"])

        return summary

    except Exception as e:
        return {"error": str(e)}
    





@app.get("/init-db")
def init_db():
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS fleet_data (
            id SERIAL PRIMARY KEY,
            truck_id TEXT,
            lat FLOAT,
            lon FLOAT,
            destination_lat FLOAT,
            destination_lon FLOAT,
            speed FLOAT,
            temperature FLOAT,
            eta_minutes FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS risk_alerts (
            id SERIAL PRIMARY KEY,
            truck_id TEXT,
            risk_score FLOAT,
            risk_level TEXT,
            time BIGINT
        );
        """)

        conn.commit()
        cur.close()
        conn.close()

        return {"message": "Tables created successfully"}

    except Exception as e:
        return {"error": str(e)}
    


import random
import threading
import time

def generate_demo_data():
    while True:
        try:
            conn = get_connection()
            cur = conn.cursor()

            truck_id = f"T{random.randint(1,5)}"
            lat = 28.5 + random.random() * 0.3
            lon = 77.1 + random.random() * 0.3
            dest_lat = 28.7
            dest_lon = 77.3
            speed = random.randint(20,80)
            temperature = random.uniform(2,10)
            eta = random.uniform(5,40)

            cur.execute("""
                INSERT INTO fleet_data
                (truck_id, lat, lon, destination_lat,
                 destination_lon, speed, temperature, eta_minutes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (truck_id, lat, lon, dest_lat, dest_lon,
                  speed, temperature, eta))

            risk_score = random.randint(10,80)
            risk_level = "CRITICAL" if risk_score > 60 else "WARNING" if risk_score > 30 else "NORMAL"

            if risk_level != "NORMAL":
                cur.execute("""
                    INSERT INTO risk_alerts
                    (truck_id, risk_score, risk_level, time)
                    VALUES (%s,%s,%s,EXTRACT(EPOCH FROM NOW()))
                """, (truck_id, risk_score, risk_level))

            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            print("Demo Data Error:", e)

        time.sleep(5)


@app.on_event("startup")
def start_demo_stream():
    threading.Thread(target=generate_demo_data, daemon=True).start()