import os
import json
import sqlite3
import pandas as pd
import requests
from dotenv import load_dotenv
from supabase import create_client, Client
import kagglehub

def data_ingestion():
    # --- Load ENV variables ---
    load_dotenv()
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10000))  # Default to 10,000 if not set

    print(f"🔍 SUPABASE_URL={SUPABASE_URL}")
    print(f"🔍 SUPABASE_KEY={SUPABASE_KEY[:5]}...")  # Don't print the full key

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")

    # --- Initialize Supabase Client ---
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # --- Download dataset from Kaggle ---
    print("📦 Downloading dataset from Kaggle...")
    path = kagglehub.dataset_download("dhruvildave/new-york-city-taxi-trips-2019")
    data_dir = os.path.join(path, "2019")
    dir_list = os.listdir(data_dir)

    # --- Define Supabase Table and Schema ---
    TABLE_NAME = "raw_data"
    ALLOWED_COLUMNS = [
        "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "ratecodeid", "store_and_fwd_flag",
        "pulocationid", "dolocationid", "payment_type", "fare_amount",
        "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount"
    ]

    SQL_CREATE_TABLE = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        vendorid float,
        tpep_pickup_datetime timestamp,
        tpep_dropoff_datetime timestamp,
        passenger_count float,
        trip_distance float,
        ratecodeid float,
        store_and_fwd_flag varchar(255),
        pulocationid float,
        dolocationid float,
        payment_type float,
        fare_amount float,
        extra float,
        mta_tax float,
        tip_amount float,
        tolls_amount float,
        improvement_surcharge float,
        total_amount float
    );
    """

    # --- RPC SQL function ---
    def run_sql_via_rpc(sql: str):
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json"
            },
            data=json.dumps({"query": sql})
        )
        if response.status_code not in (200, 201, 204):
            raise Exception(f"SQL execution failed: {response.status_code}, {response.text}")
        print(f"✅ Table `{TABLE_NAME}` ensured in Supabase.")

    # Ensure table exists
    run_sql_via_rpc(SQL_CREATE_TABLE)

    # --- Process each SQLite DB ---
    for db_file in dir_list:
        db_path = os.path.join(data_dir, db_file)
        print(f"\n🔍 Processing {db_file}")

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            if not any("tripdata" in t for t in [tbl[0] for tbl in tables]):
                print(f"⚠️ 'tripdata' table not found in {db_file}")
                continue

            df = pd.read_sql_query("SELECT * FROM tripdata", conn)

            # Keep only columns matching Supabase schema
            df = df[[col for col in ALLOWED_COLUMNS if col in df.columns]]

            # Convert NaNs to None for JSON compliance
            df = df.where(pd.notnull(df), None)
            df = df.replace({float("nan"): None, pd.NA: None})

            # Convert to dict records
            records = df.to_dict(orient="records")
            total = len(records)
            print(f"📊 Total records to upload from {db_file}: {total}")

            # Upload in batches
            for i in range(0, total, BATCH_SIZE):
                batch = records[i:i + BATCH_SIZE]
                print(f"⬆️ Uploading batch {i//BATCH_SIZE + 1}: {len(batch)} records...")
                supabase.table(TABLE_NAME).insert(batch).execute()
                print(f"✅ Uploaded batch {i//BATCH_SIZE + 1}.")

        except Exception as e:
            print(f"❌ Error processing {db_file}: {e}")
        finally:
            conn.close()

# --- Entry point ---
if __name__ == "__main__":
    data_ingestion()
