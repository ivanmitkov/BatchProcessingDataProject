# storage.py

import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv(".env")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
assert SUPABASE_URL and SUPABASE_KEY, "Supabase credentials are missing."

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def upload_sqlite_to_supabase(sqlite_file_path: str, table_name: str = "raw_data"):
    print(f"📦 Processing {sqlite_file_path}")

    try:
        conn = sqlite3.connect(sqlite_file_path)
        df = pd.read_sql_query("SELECT * FROM tripdata", conn)
        conn.close()
    except Exception as e:
        print(f"❌ Failed to load data from {sqlite_file_path}: {e}")
        return

    print(f"✅ Loaded {len(df)} rows from {sqlite_file_path}")

    records = df.to_dict(orient="records")

    print(f"⬆️ Uploading {len(records)} records to Supabase table `{table_name}`...")
    batch_size = 100
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table(table_name).insert(batch).execute()
        except Exception as e:
            print(f"⚠️ Error inserting batch {i}-{i+batch_size}: {e}")
            continue

    print(f"✅ Upload to `{table_name}` complete.")

def main():
    data_dir = "/data"
    if not os.path.exists(data_dir):
        print("❌ /data directory not found.")
        return

    for file in os.listdir(data_dir):
        if file.endswith(".db"):
            sqlite_path = os.path.join(data_dir, file)
            upload_sqlite_to_supabase(sqlite_path)

if __name__ == "__main__":
    main()
