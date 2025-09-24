#!/usr/bin/env python3
"""
Daily Refunds Pipeline:
- Connects to Paystack SFTP
- Downloads today's refund CSV files
- Enforces schema
- Uploads to BigQuery
"""

import os
import sys
import fnmatch
import logging
import posixpath
from pathlib import Path
from datetime import datetime
import glob
import pandas as pd
import paramiko
from google.cloud import bigquery
from dotenv import load_dotenv

# -------------------
# Setup logging
# -------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# -------------------
# Load .env
# -------------------
load_dotenv()

# -------------------
# Config
# -------------------
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")
SFTP_REMOTE_DIR = os.getenv("SFTP_REMOTE_DIR", "/refunds")

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

PROJECT_ID = os.getenv("BQ_PROJECT")
DATASET_ID = os.getenv("BQ_DATASET")
TABLE_ID = os.getenv("BQ_TABLE")
BQ_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Today’s date pattern
TODAY_PREFIX = datetime.utcnow().strftime("%Y-%m-%d_refunds_")
logging.info(f"Looking for refund files with prefix: {TODAY_PREFIX}")

# -------------------
# BigQuery Schema
# -------------------
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


# -------------------
# Helpers
# -------------------
def connect_sftp():
    logging.info(f"Connecting to SFTP {SFTP_USER}@{SFTP_HOST}:{SFTP_PORT}")
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    logging.info("SFTP connected")
    return sftp, transport


def list_today_files(sftp):
    """List today’s refund files from remote dir"""
    try:
        names = sftp.listdir(SFTP_REMOTE_DIR)
    except IOError as e:
        logging.error(f"Failed to list directory {SFTP_REMOTE_DIR}: {e}")
        return []
    files = [n for n in names if n.startswith(TODAY_PREFIX)]
    logging.info(f"Found {len(files)} files for today")
    return files


def download_file(sftp, filename):
    remote_path = posixpath.join(SFTP_REMOTE_DIR, filename)
    local_path = DOWNLOAD_DIR / filename
    logging.info(f"Downloading {remote_path} -> {local_path}")
    sftp.get(remote_path, str(local_path))
    return local_path


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure dataframe matches the BigQuery schema exactly"""
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
            else:
                df[col] = df[col].astype(BQ_TO_PD_DTYPES[bq_type], errors="ignore")
        except Exception as e:
            logging.error(f"Failed casting {col}: {e}")
            df[col] = pd.NA
    return df[[f.name for f in BIGQUERY_SCHEMA]]


def load_csvs() -> pd.DataFrame:
    csv_files = glob.glob(str(DOWNLOAD_DIR / f"{TODAY_PREFIX}*.csv"))
    if not csv_files:
        logging.warning("No local CSVs found for today")
        return None
    dfs = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            df = enforce_schema(df)
            dfs.append(df)
        except Exception as e:
            logging.error(f"Failed processing {f}: {e}")
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def upload_to_bq(df: pd.DataFrame):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        schema=BIGQUERY_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )
    logging.info(f"Uploading {len(df)} rows to {BQ_TABLE}")
    job = client.load_table_from_dataframe(df, BQ_TABLE, job_config=job_config)
    job.result()
    logging.info(f"Upload complete. {job.output_rows} rows loaded.")


# -------------------
# Main
# -------------------
def main():
    # Step 1: SFTP → download today’s files
    try:
        sftp, transport = connect_sftp()
        files = list_today_files(sftp)
        if not files:
            logging.info("No files to download today. Exiting.")
            return
        for fname in files:
            download_file(sftp, fname)
    except Exception as e:
        logging.error(f"SFTP step failed: {e}")
        sys.exit(1)
    finally:
        try:
            sftp.close()
            transport.close()
        except Exception:
            pass

    # Step 2: Load CSVs and enforce schema
    df = load_csvs()
    if df is None:
        logging.error("No data to upload. Exiting.")
        return

    # Step 3: Upload to BigQuery
    upload_to_bq(df)


if __name__ == "__main__":
    main()