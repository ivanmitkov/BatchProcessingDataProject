# Batch Processing Data Project

This project demonstrates a full-stack data pipeline using Dockerized microservices for batch ingestion, transformation, and visualization of a real-world dataset. The data is sourced from Kaggle's NYC Taxi Trips 2019 and stored in Supabase, a cloud PostgreSQL backend.

## Overview

The pipeline consists of three services:

1. **Ingestion Service**  
   Downloads and extracts monthly SQLite files from Kaggle, filters relevant columns, and uploads the full dataset (~30,000 records) to the `raw_data` table in Supabase.

2. **Transformation Service**  
   Waits up to 30 minutes for ingestion to complete. Then it aggregates total fares and tips by pickup date using PySpark, and writes results to the `agg_fares_by_day` table.

3. **Visualization Service**  
   A Streamlit dashboard showing daily revenue and tip trends from the transformed data.

## Setup and Run Instructions

1. Clone the repo  
   `git clone https://github.com/ivanmitkov/BatchProcessingDataProject && cd BatchProcessingDataProject`

2. Set up `.env`  
   Create a `.env` file with your Supabase URL and service key:
   ```
   SUPABASE_URL=your_url
   SUPABASE_KEY=your_key
   ```

3. Run with Docker Compose  
   ```bash
   docker-compose up --build
   ```

4. Access the dashboard at [http://localhost:8501](http://localhost:8501)

## Expected Output

After processing, the dashboard shows a time series of total revenue and tips per day from the NYC taxi dataset.

