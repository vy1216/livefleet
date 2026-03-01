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
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT", 5432)
    )

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