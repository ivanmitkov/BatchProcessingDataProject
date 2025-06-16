import os
import datetime
import time
from dotenv import load_dotenv
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum
from supabase import create_client, Client
import requests
import json

# env vars
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
assert SUPABASE_URL and SUPABASE_KEY, "Supabase credentials missing"

# Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# rpc
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
        raise Exception(f"RPC SQL failed: {response.status_code}, {response.text}")
    print("Ensured unique constraint on `pickup_date`.")

# unique constraint
def ensure_unique_constraint():
    sql_add_constraint = """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'unique_pickup_date'
      ) THEN
        ALTER TABLE agg_fares_by_day
        ADD CONSTRAINT unique_pickup_date UNIQUE (pickup_date);
      END IF;
    END$$;
    """
    run_sql_via_rpc(sql_add_constraint)

# retry until raw_data is available
def download_raw_data():
    MAX_WAIT_MINUTES = int(os.getenv("MAX_WAIT_MINUTES", 15))
    delay_seconds = 30
    max_attempts = (MAX_WAIT_MINUTES * 60) // delay_seconds

    for attempt in range(1, max_attempts + 1):
        print(f"Attempt {attempt}/{max_attempts} to download raw_data from Supabase...")
        response = supabase.table("raw_data").select("*").limit(1000).execute()
        data = response.data

        if data:
            print(f"{len(data)} rows loaded.")
            return pd.DataFrame(data)

        print(f"No data yet. Retrying in {delay_seconds} seconds...")
        time.sleep(delay_seconds)

    raise Exception(f"No data found in raw_data after {MAX_WAIT_MINUTES} minutes.")


def spark_transform(df_pd):
    spark = SparkSession.builder \
        .appName("Spark Supabase Aggregation") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    df = spark.createDataFrame(df_pd)
    df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

    agg_df = df.groupBy("pickup_date").agg(
        _sum("total_amount").alias("total_revenue"),
        _sum("tip_amount").alias("total_tips")
    ).orderBy("pickup_date")

    return agg_df

def get_existing_dates():
    print("Checking existing dates in Supabase...")
    response = supabase.table("agg_fares_by_day").select("pickup_date").execute()
    if response.data:
        existing = set(row["pickup_date"] for row in response.data)
        print(f"Found {len(existing)} existing dates.")
        return existing
    return set()

def upload_to_output_layer(df):
    print("⬆Converting Spark → Pandas for Supabase upload...")
    agg_pd = df.toPandas()
    existing_dates = get_existing_dates()

    agg_pd = agg_pd[~agg_pd["pickup_date"].isin(existing_dates)]
    if agg_pd.empty:
        print("No new records to upload — all pickup_dates already exist.")
        return

    records = []
    for _, row in agg_pd.iterrows():
        record = {
            "pickup_date": row["pickup_date"].isoformat() if isinstance(row["pickup_date"], (datetime.date, pd.Timestamp)) else row["pickup_date"],
            "total_revenue": row["total_revenue"],
            "total_tips": row["total_tips"]
        }
        records.append(record)

    table = "agg_fares_by_day"
    print(f"Uploading {len(records)} new records to {table}...")

    batch_size = 100
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        supabase.table(table).upsert(batch, on_conflict="pickup_date").execute()

    print(f"Uploaded {len(records)} new records to {table}.")

def main():
    ensure_unique_constraint()
    df_pd = download_raw_data()
    agg_df = spark_transform(df_pd)
    agg_df.show(10)
    upload_to_output_layer(agg_df)

if __name__ == "__main__":
    main()
