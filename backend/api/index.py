import requests
import psycopg2
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================
# DATABASE CONNECTION
# ==============================

def get_connection():
    return psycopg2.connect(
        host="YOUR_DB_HOST",
        database="iot-test",
        user="postgres",
        password="YOUR_PASSWORD",
        port=5432
    )


# ==============================
# CREATE TABLES
# ==============================

def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sensor_data (
        id SERIAL PRIMARY KEY,
        node_id VARCHAR(50),
        field1 FLOAT,
        field2 FLOAT,
        created_at TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tank_sensorparameters (
        id SERIAL PRIMARY KEY,
        node_id VARCHAR(50),
        tank_height_cm FLOAT,
        tank_length_cm FLOAT,
        tank_width_cm FLOAT,
        lat FLOAT,
        long FLOAT
    )
    """)

    conn.commit()
    cur.close()
    conn.close()


# ==============================
# THINGSPEAK CONFIG
# ==============================

NODE_ID = "NODE_001"

url = "https://api.thingspeak.com/channels/3290444/feeds.json?api_key=AWP8F08WA7SLO5EQ&results=1"


# ==============================
# COLLECT SENSOR DATA API
# ==============================

@app.get("/collect-data")
def collect_sensor_data():

    response = requests.get(url)
    data = response.json()

    feed = data["feeds"][0]

    distance = float(feed["field1"])
    temperature = float(feed["field2"])
    created_at = datetime.now()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO sensor_data
    (node_id, field1, field2, created_at)
    VALUES (%s,%s,%s,%s)
    """, (NODE_ID, distance, temperature, created_at))

    conn.commit()

    cur.close()
    conn.close()

    return {"message": "Sensor data inserted"}


# ==============================
# REQUEST MODEL
# ==============================

class TankParameters(BaseModel):

    node_id: str
    tank_height_cm: float
    tank_length_cm: float
    tank_width_cm: float
    lat: float
    long: float


# ==============================
# INSERT TANK PARAMETERS
# ==============================

@app.post("/tank-parameters")
def create_tank_parameters(data: TankParameters):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO tank_sensorparameters
    (node_id, tank_height_cm, tank_length_cm, tank_width_cm, lat, long)
    VALUES (%s,%s,%s,%s,%s,%s)
    RETURNING id
    """,
    (
        data.node_id,
        data.tank_height_cm,
        data.tank_length_cm,
        data.tank_width_cm,
        data.lat,
        data.long
    ))

    new_id = cur.fetchone()[0]

    conn.commit()

    cur.close()
    conn.close()

    return {"message": "Inserted", "id": new_id}


# ==============================
# GET TANK PARAMETERS
# ==============================

@app.get("/tank-parameters")
def get_tank_parameters():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM tank_sensorparameters")

    rows = cur.fetchall()

    cur.close()
    conn.close()

    result = []

    for row in rows:
        result.append({
            "id": row[0],
            "node_id": row[1],
            "tank_height_cm": row[2],
            "tank_length_cm": row[3],
            "tank_width_cm": row[4],
            "lat": row[5],
            "long": row[6]
        })

    return result


# ==============================
# GET SENSOR DATA
# ==============================

@app.get("/sensor-data")
def get_sensor_data(node_id: str = None):

    conn = get_connection()
    cur = conn.cursor()

    if node_id:
        cur.execute("""
        SELECT id,node_id,field1,field2,created_at
        FROM sensor_data
        WHERE node_id = %s
        ORDER BY created_at DESC
        """, (node_id,))
    else:
        cur.execute("""
        SELECT id,node_id,field1,field2,created_at
        FROM sensor_data
        ORDER BY created_at DESC
        """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    result = []

    for row in rows:
        result.append({
            "id": row[0],
            "node_id": row[1],
            "distance": row[2],
            "temperature": row[3],
            "created_at": row[4]
        })

    return result


# ==============================
# STARTUP EVENT
# ==============================

@app.on_event("startup")
def startup():
    create_tables()