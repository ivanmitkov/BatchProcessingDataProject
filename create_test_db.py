# create_test_db.py
import sqlite3
import pandas as pd
import os

data = {
    "vendorid": [1],
    "tpep_pickup_datetime": ["2020-01-01 00:15:00"],
    "tpep_dropoff_datetime": ["2020-01-01 00:25:00"],
    "passenger_count": [1],
    "trip_distance": [3.5],
    "ratecodeid": [1],
    "store_and_fwd_flag": ["N"],
    "pulocationid": [238],
    "dolocationid": [236],
    "payment_type": [1],
    "fare_amount": [12.5],
    "extra": [1.0],
    "mta_tax": [0.5],
    "tip_amount": [2.5],
    "tolls_amount": [0.0],
    "improvement_surcharge": [0.3],
    "total_amount": [16.8],
}

df = pd.DataFrame(data)

os.makedirs("data", exist_ok=True)
conn = sqlite3.connect("data/test.db")
df.to_sql("tripdata", conn, index=False, if_exists="replace")
conn.close()
print("✅ Test SQLite DB created at data/test.db")
