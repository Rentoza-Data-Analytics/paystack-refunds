#!/usr/bin/env python3
"""
Connects to Paystack SFTP, reads refund CSVs directly into pandas,
and uploads them into BigQuery. Logs start and end time.
"""

import os
import sys
import fnmatch
import logging
import posixpath
import io
from datetime import datetime
import decimal

import pandas as pd
import paramiko
from dotenv import load_dotenv
from google.cloud import bigquery

# =======================
# Setup
# =======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
load_dotenv()

# SFTP config
SFTP_HOST = os.getenv("SFTP_HOST", "rentoza-sftp.production.paystack.co")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USER", "sftp_rentoza")
SFTP_PASS = os.getenv("SFTP_PASS")
SFTP_REMOTE_DIR = os.getenv("SFTP_REMOTE_DIR", "/refunds")
SFTP_REMOTE_PATTERN = os.getenv("SFTP_REMOTE_PATTERN", "*.csv")

# BigQuery config
PROJECT_ID = os.getenv("BQ_PROJECT")
DATASET_ID = os.getenv("BQ_DATASET")
TABLE_ID = os.getenv("BQ_TABLE")
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

# Pandas dtype mapping
BQ_TO_PD_DTYPES = {
    "STRING": "string",
    "INTEGER": "Int64",
    "NUMERIC": "float64",
    "BOOLEAN": "boolean",
    "DATE": "string",
    "TIMESTAMP": "datetime64[ns]",
}

# =======================
# Helpers
# =======================
def connect_sftp(host, port, username, password):
    logging.info(f"Connecting to SFTP {username}@{host}:{port}")
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    logging.info("SFTP connected")
    return sftp, transport

def list_remote_files(sftp, remote_dir, pattern):
    files = []
    for name in sftp.listdir(remote_dir):
        if fnmatch.fnmatch(name, pattern):
            files.append(name)
    logging.info(f"Found {len(files)} files in {remote_dir} matching '{pattern}'")
    return files

def read_csv_from_sftp(sftp, remote_dir, filename) -> pd.DataFrame:
    remote_path = posixpath.join(remote_dir, filename)
    logging.info(f"Reading {remote_path} from SFTP")
    flike = io.BytesIO()
    sftp.getfo(remote_path, flike)   # fetch file directly into memory buffer
    flike.seek(0)
    df = pd.read_csv(flike)
    return df

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    for field in BIGQUERY_SCHEMA:
        col, bq_type = field.name, field.field_type
        if col not in df.columns:
            df[col] = pd.NA
        try:
            if bq_type == "TIMESTAMP":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif bq_type == "DATE":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif bq_type == "NUMERIC":
                df[col] = df[col].astype("float64").astype(str).map(decimal.Decimal)
            else:
                df[col] = df[col].astype(BQ_TO_PD_DTYPES[bq_type], errors="ignore")
        except Exception as e:
            logging.error(f"Failed casting {col}: {e}")
            df[col] = pd.NA
    return df[[f.name for f in BIGQUERY_SCHEMA]]

def upload_to_bigquery(df: pd.DataFrame, table: str):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        schema=BIGQUERY_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )
    logging.info(f"Uploading {len(df)} rows to {table} ...")
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()
    logging.info(f"Upload complete. {job.output_rows} rows loaded.")

# =======================
# Main
# =======================
def main():
    start_time = datetime.now()
    logging.info(f"Process started at {start_time}")

    # Step 1: Download & merge into DataFrame
    try:
        sftp, transport = connect_sftp(SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASS)
    except Exception as e:
        logging.error("Failed to connect to SFTP: %s", e)
        sys.exit(1)

    df_list = []
    try:
        files = list_remote_files(sftp, SFTP_REMOTE_DIR, SFTP_REMOTE_PATTERN)
        for fname in files:
            try:
                df = read_csv_from_sftp(sftp, SFTP_REMOTE_DIR, fname)
                df = enforce_schema(df)
                df_list.append(df)
            except Exception as e:
                logging.error(f"Failed processing {fname}: {e}")
    finally:
        try:
            sftp.close()
            transport.close()
        except Exception:
            pass
        logging.info("Closed SFTP connection")

    if not df_list:
        logging.error("No data to upload")
        return

    # Step 2: Upload to BigQuery
    df = pd.concat(df_list, ignore_index=True)
    upload_to_bigquery(df, BQ_TABLE)

    end_time = datetime.now()
    logging.info(f"Process finished at {end_time}")
    logging.info(f"Total duration: {end_time - start_time}")

if __name__ == "__main__":
    main()