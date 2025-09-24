#!/usr/bin/env python3
"""
Reads refund CSVs from 'downloads/' and loads them into BigQuery table.
Enforces schema datatypes to avoid mismatches.
"""

import os
import glob
import logging
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Load env variables
load_dotenv()

# Config
DOWNLOAD_DIR = "downloads"
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = os.getenv("BIGQUERY_DATASET")
TABLE_ID = os.getenv("BIGQUERY_TABLE")

# Full table path
BQ_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# BigQuery schema
BIGQUERY_SCHEMA = [
    bigquery.SchemaField("unique_id", "STRING"),
    bigquery.SchemaField("refund_id", "INTEGER"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("integration", "INTEGER"),
    bigquery.SchemaField("business_name", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("domain", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("transaction", "INTEGER"),
    bigquery.SchemaField("dispute", "INTEGER"),
    bigquery.SchemaField("channel", "STRING"),
    bigquery.SchemaField("fully_deducted", "BOOLEAN"),
    bigquery.SchemaField("refunded_by", "STRING"),
    bigquery.SchemaField("refunded_at", "TIMESTAMP"),
    bigquery.SchemaField("expected_at", "TIMESTAMP"),
    bigquery.SchemaField("deducted_amount", "NUMERIC"),
    bigquery.SchemaField("amount", "NUMERIC"),
    bigquery.SchemaField("customer_note", "STRING"),
    bigquery.SchemaField("merchant_note", "STRING"),
    bigquery.SchemaField("transaction_reference", "STRING"),
    bigquery.SchemaField("settlement_date", "DATE"),
    bigquery.SchemaField("settled", "BOOLEAN"),
    bigquery.SchemaField("customer_id", "INTEGER"),
    bigquery.SchemaField("customer_email", "STRING"),
    bigquery.SchemaField("customer_phone", "STRING"),
    bigquery.SchemaField("customer_first_name", "STRING"),
    bigquery.SchemaField("customer_last_name", "STRING"),
    bigquery.SchemaField("customer_code", "STRING"),
    bigquery.SchemaField("merge_timestamp", "DATE"),
]

# Mapping from BigQuery type → pandas dtype
BQ_TO_PD_DTYPES = {
    "STRING": "string",
    "INTEGER": "Int64",
    "NUMERIC": "float64",
    "BOOLEAN": "boolean",
    "DATE": "string",
    "TIMESTAMP": "datetime64[ns]",
}


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure dataframe matches the BigQuery schema exactly"""
    # Normalize column names: strip spaces, unify casing
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')  # remove leading/trailing spaces

    for field in BIGQUERY_SCHEMA:
        col, bq_type = field.name, field.field_type

        # Add missing column
        if col not in df.columns:
            logging.warning(f"Adding missing column: {col}")
            df[col] = pd.NA

        # Apply type conversion
        try:
            if bq_type == "TIMESTAMP":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif bq_type == "DATE":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            else:
                df[col] = df[col].astype(BQ_TO_PD_DTYPES[bq_type], errors="ignore")
        except Exception as e:
            logging.error(f"Failed to cast {col} to {bq_type}: {e}")
            df[col] = pd.NA

    # Drop extra columns not in schema
    df = df[[f.name for f in BIGQUERY_SCHEMA]]
    return df


def load_csv_files(download_dir: str) -> pd.DataFrame:
    """Read and merge all CSV files from downloads folder"""
    csv_files = glob.glob(os.path.join(download_dir, "*.csv"))
    if not csv_files:
        logging.error("No CSV files found in downloads/")
        return None

    logging.info(f"Found {len(csv_files)} CSV files")
    df_list = []
    for f in csv_files:
        try:
            logging.info(f"Reading {f}")
            df = pd.read_csv(f)
            df = enforce_schema(df)
            df_list.append(df)
        except Exception as e:
            logging.error(f"Failed to process {f}: {e}")
            continue

    if not df_list:
        return None
    return pd.concat(df_list, ignore_index=True)


def upload_to_bigquery(df: pd.DataFrame, table: str):
    """Upload dataframe to BigQuery"""
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema=BIGQUERY_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )

    logging.info(f"Uploading {len(df)} rows to {table} ...")
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()  # wait for job to complete
    logging.info(f"Upload complete. {job.output_rows} rows loaded.")


def main():
    df = load_csv_files(DOWNLOAD_DIR)
    if df is None:
        logging.error("No data to upload. Exiting.")
        return

    upload_to_bigquery(df, BQ_TABLE)


if __name__ == "__main__":
    main()