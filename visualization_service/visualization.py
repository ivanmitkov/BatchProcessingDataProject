import streamlit as st
import pandas as pd
import requests
from dotenv import load_dotenv
import os

# Load credentials from .env
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Fetch data from Supabase
@st.cache_data(ttl=300)
def fetch_data():
    url = f"{SUPABASE_URL}/rest/v1/agg_fares_by_day?select=*"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        st.error(f"Error fetching data: {r.status_code}")
        return pd.DataFrame()
    df = pd.DataFrame(r.json())
    if "pickup_date" in df.columns:
        df["pickup_date"] = pd.to_datetime(df["pickup_date"])
        df = df.sort_values("pickup_date")  # Sort by date
    return df

# Load and display
st.title("NYC Taxi Aggregated Revenue")

df = fetch_data()
if df.empty:
    st.warning("No data available.")
else:
    st.dataframe(df)
    st.line_chart(df.set_index("pickup_date")[["total_revenue", "total_tips"]])
